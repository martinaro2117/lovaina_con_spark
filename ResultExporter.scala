package louvain

import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Exporta los resultados del algoritmo Louvain a disco y muestra
 * estadísticas básicas de las comunidades detectadas.
 *
 * Salida:
 *   <outputPath>/communities.csv   — screen_name, community_id, community_size
 *   <outputPath>/stats.csv         — community_id, size, top_members
 */
object ResultExporter {

  def save(
    spark      : SparkSession,
    communities: VertexRDD[Long],
    idToName   : RDD[(Long, String)],
    outputPath : String
  ): Unit = {

    import spark.implicits._

    val sc = spark.sparkContext

    // ── Unir communityId con screen_name ──────────────────────────────────
    // communities: RDD[(vertexId, communityId)]
    // idToName   : RDD[(vertexId, screen_name)]
    val labeled: RDD[(String, Long)] = communities
      .join(idToName)
      .map { case (_, (commId, name)) => (name, commId) }

    // ── Tamaño de cada comunidad ──────────────────────────────────────────
    val commSizes: RDD[(Long, Long)] = labeled
      .map { case (_, commId) => (commId, 1L) }
      .reduceByKey(_ + _)

    val commSizeMap = sc.broadcast(commSizes.collectAsMap())

    // ── CSV principal: un registro por usuario ────────────────────────────
    labeled
      .map { case (name, commId) =>
        val size = commSizeMap.value.getOrElse(commId, 0L)
        s"$name,$commId,$size"
      }
      .repartition(1)
      .saveAsTextFile(s"$outputPath/communities_raw")

    // ── Estadísticas de comunidades (top 20) ──────────────────────────────
    val topCommunities = commSizes
      .sortBy(_._2, ascending = false)
      .take(20)

    println("\n=== Top 20 comunidades por tamaño ===")
    println(f"${"community_id"}%-20s ${"size"}%8s")
    println("-" * 30)
    topCommunities.foreach { case (id, size) =>
      println(f"$id%-20s $size%8d")
    }

    // ── DataFrame con nombre de comunidad y 5 miembros representativos ────
    val topMembers = labeled
      .groupBy(_._2)
      .mapValues(_.map(_._1).take(5).mkString("|"))

    val statsDF = commSizes
      .join(topMembers)
      .map { case (commId, (size, members)) =>
        (commId, size, members)
      }
      .toDF("community_id", "size", "sample_members")
      .orderBy($"size".desc)

    statsDF
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"$outputPath/stats")

    commSizeMap.destroy()

    println(s"\n✓ Exportación completa → $outputPath")
  }
}
