package louvain

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Implementación del algoritmo de Louvain sobre GraphX.
 *
 * El algoritmo alterna dos fases hasta converger:
 *
 *   Fase 1 (modularity optimization)
 *     Cada nodo se mueve a la comunidad vecina que produce el mayor
 *     incremento de modularidad ΔQ. Se repite hasta que ningún movimiento
 *     mejore Q.  Implementado con aggregateMessages + joinVertices.
 *
 *   Fase 2 (community aggregation)
 *     Cada comunidad se contrae en un supernodo. Las aristas inter-comunidad
 *     se suman. El grafo resultante es más pequeño y se pasa de nuevo a Fase 1.
 *
 * Referencias:
 *   Blondel et al. (2008) "Fast unfolding of communities in large networks"
 */
object LouvainAlgorithm {

  // Tipo de atributo de vértice: communityId (Long)
  type CommGraph = Graph[Long, Double]

  /**
   * Ejecuta Louvain hasta convergencia o maxIter iteraciones.
   *
   * @return (VertexRDD con communityId final, modularidad Q final)
   */
  def run(
    graph  : CommGraph,
    maxIter: Int,
    epsilon: Double
  ): (VertexRDD[Long], Double) = {

    var current  = graph
    var prevQ    = Double.NegativeInfinity
    var iter     = 0
    var continue = true

    while (continue && iter < maxIter) {
      iter += 1
      println(s"\n--- Iteración $iter ---")

      // ── Fase 1: optimización local ───────────────────────────────────
      val afterPhase1 = phase1(current)

      // ── Calcular modularidad para decidir si seguir ──────────────────
      val m  = totalWeight(afterPhase1)
      val Q  = modularity(afterPhase1, m)
      println(s"  Q después de fase 1 = ${"%.6f".format(Q)}")

      if (Q - prevQ < epsilon) {
        // Convergió: ninguna asignación mejora Q de forma significativa
        println(s"  Convergencia alcanzada (ΔQ < $epsilon)")
        continue = false
        current  = afterPhase1
        prevQ    = Q
      } else {
        prevQ = Q

        // ── Fase 2: contracción del grafo ────────────────────────────
        val afterPhase2 = phase2(afterPhase1)
        println(s"  Supergrafo: ${afterPhase2.vertices.count()} nodos, " +
                s"${afterPhase2.edges.count()} aristas")

        // Liberar versión anterior para evitar lineage explosivo
        current.unpersist()
        current = afterPhase2.cache()
      }
    }

    (current.vertices, prevQ)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Fase 1 — Mover nodos a la comunidad vecina que maximiza ΔQ
  // ─────────────────────────────────────────────────────────────────────────
  /**
   * Para cada vértice v calculamos el peso acumulado hacia cada comunidad
   * vecina usando aggregateMessages.  Luego asignamos v a la comunidad con
   * mayor peso acumulado (heurística de primer paso de Louvain).
   *
   * aggregateMessages es el primitivo central de GraphX: envía mensajes
   * a lo largo de las aristas y los fusiona en los vértices de destino,
   * todo en paralelo y sin necesidad de iterar sobre el grafo en el driver.
   */
  private def phase1(g: CommGraph): CommGraph = {

    // Mensaje = Map[communityId → peso_acumulado]
    // Cada arista contribuye el peso a la comunidad del extremo contrario.
    val communityWeights: VertexRDD[Map[Long, Double]] =
      g.aggregateMessages[Map[Long, Double]](
        sendMsg = ctx => {
          // src informa a dst de la comunidad de src y el peso de la arista
          ctx.sendToDst(Map(ctx.srcAttr -> ctx.attr))
          // dst informa a src de la comunidad de dst
          ctx.sendToSrc(Map(ctx.dstAttr -> ctx.attr))
        },
        mergeMsg = mergeMaps
      )

    // Para cada vértice, elige la comunidad con mayor peso acumulado.
    // Si el vértice no recibió ningún mensaje (nodo aislado) mantiene su id.
    val newCommunities: VertexRDD[Long] = communityWeights.mapValues { weightMap =>
      weightMap.maxBy(_._2)._1
    }

    // joinVertices actualiza el atributo de cada vértice con el nuevo communityId
    g.joinVertices(newCommunities)((_, _, newComm) => newComm)
  }

  // Fusión de mapas acumulando pesos por comunidad
  private def mergeMaps(a: Map[Long, Double], b: Map[Long, Double]): Map[Long, Double] = {
    val merged = scala.collection.mutable.Map(a.toSeq: _*)
    b.foreach { case (k, v) => merged(k) = merged.getOrElse(k, 0.0) + v }
    merged.toMap
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Fase 2 — Contraer cada comunidad en un supernodo
  // ─────────────────────────────────────────────────────────────────────────
  /**
   * Cada comunidad pasa a ser un único nodo en el supergrafo.
   * Las aristas entre nodos de la misma comunidad desaparecen (son intra-comunidad).
   * Las aristas entre comunidades distintas se suman en una sola arista ponderada.
   *
   * Esto reduce el tamaño del grafo de forma exponencial entre iteraciones,
   * lo que es la clave de la eficiencia de Louvain.
   */
  private def phase2(g: CommGraph): CommGraph = {

    val sc = g.vertices.sparkContext

    // Aristas entre comunidades distintas, sumando pesos
    val superEdges: RDD[Edge[Double]] = g.triplets
      .filter(t => t.srcAttr != t.dstAttr)
      .map(t => ((math.min(t.srcAttr, t.dstAttr),
                  math.max(t.srcAttr, t.dstAttr)), t.attr))
      .reduceByKey(_ + _)
      .map { case ((u, v), w) => Edge(u, v, w) }

    // Supernodos: un vértice por communityId, atributo = propio id
    val superVertices: RDD[(VertexId, Long)] = superEdges
      .flatMap(e => Seq(e.srcId, e.dstId))
      .distinct()
      .map(id => (id, id))

    Graph(superVertices, superEdges)
      .partitionBy(PartitionStrategy.EdgePartition2D)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Modularidad Q
  // ─────────────────────────────────────────────────────────────────────────
  /**
   * Q = Σ_{c} [ L_c/m  −  (d_c / 2m)² ]
   *
   * donde:
   *   L_c  = suma de pesos de aristas intra-comunidad c
   *   d_c  = suma de grados ponderados de nodos en c
   *   m    = suma total de pesos de todas las aristas
   *
   * Un Q cercano a 1 indica comunidades muy densas internamente.
   * Un Q ≈ 0 indica estructura aleatoria.
   */
  def modularity(g: CommGraph, m: Double): Double = {
    if (m == 0) return 0.0

    // Grado ponderado de cada vértice
    val weightedDegree: VertexRDD[Double] = g.aggregateMessages[Double](
      sendMsg = ctx => {
        ctx.sendToSrc(ctx.attr)
        ctx.sendToDst(ctx.attr)
      },
      mergeMsg = _ + _
    )

    // Suma de grados ponderados por comunidad
    val communityDegree: RDD[(Long, Double)] = g.vertices
      .join(weightedDegree)
      .map { case (_, (comm, deg)) => (comm, deg) }
      .reduceByKey(_ + _)

    val communityDegreeMap = g.vertices.sparkContext
      .broadcast(communityDegree.collectAsMap())

    // Suma de pesos de aristas intra-comunidad
    val intraWeight: RDD[(Long, Double)] = g.triplets
      .filter(t => t.srcAttr == t.dstAttr)
      .map(t => (t.srcAttr, t.attr))
      .reduceByKey(_ + _)

    // Q = Σ_c [ lc/m - (dc/2m)^2 ]
    val Q = intraWeight
      .map { case (comm, lc) =>
        val dc = communityDegreeMap.value.getOrElse(comm, 0.0)
        lc / m - math.pow(dc / (2.0 * m), 2)
      }
      .sum()

    communityDegreeMap.destroy()
    Q
  }

  /** Suma total de pesos de todas las aristas del grafo. */
  def totalWeight(g: CommGraph): Double =
    g.edges.map(_.attr).sum()
}
