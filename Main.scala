package louvain

import org.apache.spark.sql.SparkSession

/**
 * Punto de entrada del pipeline Louvain sobre datos de Twitter.
 *
 * Uso:
 *   spark-submit --master local[*] louvain-graphx-assembly-1.0.0.jar \
 *     --input  data/tweets.jsonl \
 *     --output data/communities \
 *     --min-weight 2.0 \
 *     --max-iter   15 \
 *     --epsilon    0.0001
 */
object Main {

  case class Config(
    inputPath : String  = "data/tweets.jsonl",
    outputPath: String  = "data/communities",
    minWeight : Double  = 1.0,
    maxIter   : Int     = 20,
    epsilon   : Double  = 1e-4
  )

  def main(args: Array[String]): Unit = {

    // ── Parseo de argumentos ──────────────────────────────────────────────
    val cfg = parseArgs(args)

    // ── Sesión Spark ──────────────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Louvain-GraphX-Twitter")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      // En Windows local, evitar problemas con Hadoop winutils usando tmp en memoria
      .config("spark.local.dir", System.getProperty("java.io.tmpdir"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"\n=== Louvain-GraphX | input: ${cfg.inputPath} ===\n")

    // ── 1. Ingesta y construcción del grafo ───────────────────────────────
    val (graph, userIndex) = GraphBuilder.build(
      spark,
      jsonlPath = cfg.inputPath,
      minWeight = cfg.minWeight
    )
    println(s"Grafo inicial: ${graph.vertices.count()} vértices, ${graph.edges.count()} aristas")

    // ── 2. Algoritmo de Louvain iterativo ─────────────────────────────────
    val (communities, finalQ) = LouvainAlgorithm.run(
      graph   = graph,
      maxIter = cfg.maxIter,
      epsilon = cfg.epsilon
    )
    println(s"\nModularidad final Q = ${"%.6f".format(finalQ)}")
    println(s"Comunidades detectadas: ${communities.values.distinct().count()}")

    // ── 3. Exportar resultados ────────────────────────────────────────────
    ResultExporter.save(spark, communities, userIndex, cfg.outputPath)
    println(s"\n✓ Resultados guardados en ${cfg.outputPath}")

    spark.stop()
  }

  // ── Parseo simple de argumentos clave-valor ───────────────────────────
  private def parseArgs(args: Array[String]): Config = {
    val map = args.sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap
    Config(
      inputPath  = map.getOrElse("--input",      "data/tweets.jsonl"),
      outputPath = map.getOrElse("--output",     "data/communities"),
      minWeight  = map.getOrElse("--min-weight", "1.0").toDouble,
      maxIter    = map.getOrElse("--max-iter",   "20").toInt,
      epsilon    = map.getOrElse("--epsilon",    "0.0001").toDouble
    )
  }
}
