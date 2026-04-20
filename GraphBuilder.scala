package louvain

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Lee tweets.jsonl (producido por el notebook Python del dataset),
 * extrae relaciones de retweet y mención entre usuarios y construye
 * un Graph[Long, Double] de GraphX donde:
 *
 *   - Vértice  = usuario (VertexId numérico, mapeado desde screen_name)
 *   - Arista   = interacción usuario→usuario con peso = nº de interacciones
 *   - El grafo se trata como NO dirigido (aristas duplicadas y fusionadas)
 *
 * Se expone también el índice screen_name→VertexId para poder
 * interpretar los resultados al final.
 */
object GraphBuilder {

  /**
   * @param spark      sesión activa
   * @param jsonlPath  ruta al archivo tweets.jsonl
   * @param minWeight  umbral mínimo de peso; aristas más débiles se descartan
   * @return           (grafo GraphX, RDD[(VertexId, screen_name)])
   */
  def build(
    spark     : SparkSession,
    jsonlPath : String,
    minWeight : Double
  ): (Graph[Long, Double], RDD[(VertexId, String)]) = {

    import spark.implicits._

    // ── 1. Leer JSONL ─────────────────────────────────────────────────────
    // spark.read.json infiere el esquema automáticamente desde JSONL.
    // Se seleccionan sólo los campos necesarios para reducir memoria.
    val raw = spark.read.json(jsonlPath)

    // ── 2. Extraer pares (src_screen_name, dst_screen_name) ───────────────
    // Fuentes de interacción que usamos:
    //   a) Retweets  : el autor del tweet → el autor del tweet original
    //   b) Menciones : el autor del tweet → cada usuario mencionado
    //
    // Ambas fuentes se unen en un único DataFrame de pares de strings.

    // a) Retweets: retweeted_status.user.screen_name existe cuando es un RT
    val retweets = raw
      .filter(F.col("retweeted_status").isNotNull)
      .select(
        F.col("user.screen_name")                          .alias("src"),
        F.col("retweeted_status.user.screen_name")         .alias("dst")
      )
      .filter(F.col("src").isNotNull && F.col("dst").isNotNull)
      .filter(F.col("src") =!= F.col("dst"))

    // b) Menciones: entities.user_mentions es un array de structs
    val mentions = raw
      .filter(F.size(F.col("entities.user_mentions")) > 0)
      .select(
        F.col("user.screen_name").alias("src"),
        F.explode(F.col("entities.user_mentions.screen_name")).alias("dst")
      )
      .filter(F.col("src").isNotNull && F.col("dst").isNotNull)
      .filter(F.col("src") =!= F.col("dst"))

    val pairs = retweets.union(mentions)

    // ── 3. Normalizar a pares no dirigidos y agregar pesos ─────────────────
    // Para que el grafo sea no dirigido unificamos (A,B) y (B,A) en un solo par
    // ordenando lexicográficamente. Luego contamos ocurrencias como peso.
    val weightedPairs = pairs
      .select(
        F.when(F.col("src") < F.col("dst"), F.col("src")).otherwise(F.col("dst")).alias("u"),
        F.when(F.col("src") < F.col("dst"), F.col("dst")).otherwise(F.col("src")).alias("v")
      )
      .groupBy("u", "v")
      .agg(F.count("*").cast("double").alias("weight"))
      .filter(F.col("weight") >= minWeight)
      .cache()

    // ── 4. Construir índice string → VertexId (Long) ──────────────────────
    // GraphX requiere VertexId de tipo Long. Asignamos un índice denso
    // usando zipWithIndex sobre el conjunto de nombres únicos.
    val allNames: RDD[String] = weightedPairs
      .select(F.col("u")).union(weightedPairs.select(F.col("v")))
      .distinct()
      .rdd
      .map(_.getString(0))

    // nameToId: broadcast map para traducir strings a Long en las aristas
    val nameToIdRDD: RDD[(String, VertexId)] =
      allNames.zipWithIndex().map { case (name, idx) => (name, idx) }

    val nameToIdMap = spark.sparkContext.broadcast(
      nameToIdRDD.collectAsMap()
    )

    // Índice inverso para exportar resultados legibles
    val idToNameRDD: RDD[(VertexId, String)] =
      nameToIdRDD.map { case (name, id) => (id, name) }

    // ── 5. Construir RDDs de vértices y aristas ────────────────────────────
    val vertices: RDD[(VertexId, Long)] = nameToIdRDD
      .map { case (_, id) => (id, id) }   // atributo inicial = propio id (comunidad)

    val edges: RDD[Edge[Double]] = weightedPairs.rdd.flatMap { row =>
      val map = nameToIdMap.value
      for {
        srcId <- map.get(row.getString(0))
        dstId <- map.get(row.getString(1))
      } yield Edge(srcId, dstId, row.getDouble(2))
    }

    // ── 6. Instanciar Graph y aplicar particionado ─────────────────────────
    // EdgePartition2D minimiza el tráfico de red en operaciones tipo
    // aggregateMessages al colocar aristas que comparten vértices
    // en la misma partición siempre que sea posible.
    val graph = Graph(vertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .cache()

    weightedPairs.unpersist()

    (graph, idToNameRDD)
  }
}
