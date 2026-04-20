# Louvain sobre GraphX — Documentación del proyecto

## Índice

1. [Visión general](#visión-general)
2. [Por qué GraphX y no PySpark](#por-qué-graphx-y-no-pyspark)
3. [Arquitectura del proyecto](#arquitectura-del-proyecto)
4. [Pipeline paso a paso](#pipeline-paso-a-paso)
   - [Preparación de datos (Python)](#preparación-de-datos-python)
   - [GraphBuilder — construcción del grafo](#graphbuilder--construcción-del-grafo)
   - [LouvainAlgorithm — el algoritmo](#louvainalgoritm--el-algoritmo)
   - [ResultExporter — exportar resultados](#resultexporter--exportar-resultados)
5. [Conceptos clave de GraphX](#conceptos-clave-de-graphx)
6. [Por qué Louvain y no otros algoritmos](#por-qué-louvain-y-no-otros-algoritmos)
7. [Instrucciones de ejecución (Windows + VSCode)](#instrucciones-de-ejecución-windows--vscode)
8. [Interpretación de resultados](#interpretación-de-resultados)
9. [Limitaciones y posibles mejoras](#limitaciones-y-posibles-mejoras)

---

## Visión general

Este proyecto detecta **comunidades de usuarios de Twitter** a partir de sus interacciones (retweets y menciones). Se utiliza el **algoritmo de Louvain** para maximizar la *modularidad* del grafo, una medida de la calidad de las comunidades detectadas.

El grafo se representa y procesa con **Apache Spark GraphX**, lo que permite paralelizar tanto la construcción del grafo como cada fase del algoritmo sobre múltiples cores (en local) o nodos (en cluster).

---

## Por qué GraphX y no PySpark

Esta es la pregunta clave del proyecto. La respuesta tiene tres dimensiones:

### 1. Modelo de datos

PySpark trabaja con **DataFrames y RDDs de filas**. Cuando se trata de algoritmos de grafo, los datos tienen dos tipos de entidad con semántica muy diferente: **vértices** y **aristas**. En PySpark puro, un grafo se representaría como dos DataFrames separados (uno de nodos, otro de aristas). Para hacer una operación de "enviar mensajes a lo largo de aristas" habría que hacer múltiples `join` manuales entre ambos DataFrames.

GraphX tiene un tipo de datos propio, `Graph[VD, ED]`, que mantiene vértices y aristas **colocalizados** en particiones relacionadas. Esto hace que operaciones de propagación (como `aggregateMessages`) sean órdenes de magnitud más eficientes porque evitan shuffles innecesarios.

### 2. La primitiva `aggregateMessages`

El corazón del algoritmo de Louvain es: *"para cada nodo, saber cuánto peso de aristas conecta con cada comunidad vecina"*. Eso requiere propagación de mensajes a lo largo de aristas.

Con PySpark puro, la operación equivalente sería:

```python
# PySpark: requiere 2 joins + 2 groupBy = mucho shuffle
neighbors = edges_df.join(nodes_df, edges_df.src == nodes_df.id)
weighted   = neighbors.groupBy("dst", "community").agg(sum("weight"))
best_comm  = weighted.groupBy("dst").agg(max_by("community", "total"))
```

Con GraphX:

```scala
// GraphX: una sola pasada sobre las aristas, sin joins explícitos
graph.aggregateMessages[Map[Long, Double]](
  sendMsg  = ctx => ctx.sendToDst(Map(ctx.srcAttr -> ctx.attr)),
  mergeMsg = mergeMaps
)
```

La diferencia no es sólo sintáctica. `aggregateMessages` ejecuta en el mismo executor donde viven las aristas, reduciendo el movimiento de datos. Un join de PySpark siempre implica un shuffle completo.

### 3. GraphFrames (la alternativa Python)

Existe `graphframes`, una librería Python que envuelve GraphX. Proporciona algoritmos predefinidos como PageRank o Label Propagation, pero **no implementa Louvain**. Además, para algoritmos personalizados, GraphFrames obliga a definir la lógica en términos de SQL/DataFrame lo que vuelve al problema del punto 2.

Para un algoritmo iterativo personalizado como Louvain, implementarlo directamente en Scala sobre GraphX primitivo es la opción más eficiente y expresiva.

| Criterio | PySpark puro | GraphFrames | GraphX (Scala) |
|---|---|---|---|
| Louvain disponible | No | No | Sí (implementación propia) |
| `aggregateMessages` | No | Limitado | Sí, nativo |
| Colocación vértices/aristas | No | Parcial | Sí |
| Rendimiento iterativo | Bajo | Medio | Alto |
| Curva de aprendizaje | Baja | Baja | Media |

---

## Arquitectura del proyecto

```
louvain-graphx/
├── build.sbt                          # Dependencias y configuración SBT
├── project/plugins.sbt                # Plugin sbt-assembly para fat JAR
├── prepare_data.py                    # Convierte .gz → JSONL (Python)
├── data/
│   ├── cache-0-json.gz                # Dataset original (ya descargado)
│   └── tweets.jsonl                   # Generado por prepare_data.py
└── src/main/scala/louvain/
    ├── Main.scala                     # Punto de entrada, orquestador
    ├── GraphBuilder.scala             # Lectura JSONL → Graph[Long, Double]
    ├── LouvainAlgorithm.scala         # Fases 1 y 2 del algoritmo
    └── ResultExporter.scala           # Exportación CSV y estadísticas
```

---

## Pipeline paso a paso

### Preparación de datos (Python)

**Archivo:** `prepare_data.py`

**Por qué existe este paso:** El dataset original está en formato JSON array comprimido (`.gz`). `spark.read.json()` puede leer JSON array, pero es mucho más eficiente con **JSONL** (JSON Lines): un tweet por línea. Esto permite a Spark dividir el archivo en bloques de forma independiente y procesarlos en paralelo sin necesidad de parsear el array completo primero.

**Qué hace:**
- Lee el `.gz` haciendo streaming (sin cargar todo en memoria)
- Filtra líneas que no sean objetos tweet
- Escribe cada tweet en una línea JSON en `tweets.jsonl`

```bash
python prepare_data.py --input data/cache-0-json.gz --output data/tweets.jsonl
```

Para pruebas rápidas se puede limitar el número de tweets:

```bash
python prepare_data.py --max 100000
```

---

### GraphBuilder — construcción del grafo

**Archivo:** `GraphBuilder.scala`

**Por qué este diseño:** El grafo de interacciones de Twitter es **no dirigido** y **ponderado**. Tratarlo como no dirigido es una decisión de diseño habitual en detección de comunidades: una mención o un retweet indica relación entre dos usuarios independientemente de quién inició la interacción.

**Qué hace paso a paso:**

**Paso 1 — Leer JSONL.** Se usa `spark.read.json()` que infiere el esquema automáticamente. Solo se seleccionan los campos necesarios para reducir la huella de memoria.

**Paso 2 — Extraer pares de interacción.** Se obtienen dos tipos de relación:
- *Retweets*: el autor retweetea al autor original. Se detectan por la presencia del campo `retweeted_status`.
- *Menciones*: el autor menciona a otro usuario. Se detectan explotando el array `entities.user_mentions`.

**Paso 3 — Normalizar a grafo no dirigido.** Los pares `(A, B)` y `(B, A)` se unifican ordenando lexicográficamente los nombres. Luego se cuentan las ocurrencias del mismo par como peso de la arista. Un par con muchas ocurrencias indica una relación fuerte.

**Paso 4 — Mapear strings a Long.** GraphX requiere que los identificadores de vértice sean de tipo `Long`. Se asigna un índice numérico a cada `screen_name` usando `zipWithIndex()`, que es una operación eficiente que no requiere recolectar todos los datos en el driver.

**Paso 5 — Construir el grafo.** Se instancia `Graph(vertices, edges)` y se aplica `partitionBy(EdgePartition2D)`. Esta estrategia de particionado asigna aristas a particiones teniendo en cuenta los dos vértices extremos, minimizando el número de copias de datos de vértices durante `aggregateMessages`.

---

### LouvainAlgorithm — el algoritmo

**Archivo:** `LouvainAlgorithm.scala`

#### El bucle principal

El algoritmo itera alternando Fase 1 y Fase 2. Se detiene cuando el incremento de modularidad entre iteraciones es menor que `epsilon` (convergencia), o cuando se alcanza `maxIter`. En cada iteración se llama a `.unpersist()` sobre el grafo anterior para liberar memoria de los executors y evitar que el *lineage* de RDDs crezca indefinidamente.

#### Fase 1 — Optimización de modularidad local

**Por qué `aggregateMessages`:** La operación necesaria es: *"para cada vértice v, calcular la suma de pesos de aristas que conectan v con cada comunidad vecina"*. Esto requiere acceder al atributo (communityId) del vértice vecino y al peso de la arista. `aggregateMessages` hace exactamente eso: para cada arista, ejecuta `sendMsg` en el mismo executor donde vive la arista (accediendo al contexto de triplete que incluye vértice origen, arista y vértice destino), y luego fusiona los mensajes en los vértices destino con `mergeMsg`.

```
Arista (A, B, peso=3.0)
  → sendToDst: Map(community(A) → 3.0)   — B aprende del vecino A
  → sendToSrc: Map(community(B) → 3.0)   — A aprende del vecino B
```

El resultado es un `VertexRDD[Map[Long, Double]]`: para cada vértice, un mapa de communityId → peso total.

**La heurística de movimiento:** Se elige la comunidad vecina con mayor peso acumulado. Esta es una aproximación al cálculo exacto de ΔQ que simplifica la implementación manteniendo resultados prácticos similares. Para el cálculo exacto de ΔQ completo habría que computar grados de comunidad, lo que añadiría otro `aggregateMessages` por iteración.

**`joinVertices`:** Actualiza el atributo de cada vértice (su communityId) con el resultado de la fase. Es una operación de join eficiente porque las particiones de vértices están colocalizadas con las particiones de aristas.

#### Fase 2 — Contracción del supergrafo

**Por qué contraer:** Tras la Fase 1, múltiples vértices tienen el mismo `communityId`. La Fase 2 los reemplaza por un único supernodo. Las aristas intra-comunidad desaparecen (ya no son relevantes para la siguiente iteración) y las aristas inter-comunidad se suman.

Esto reduce el número de nodos de forma drástica entre iteraciones. Un grafo con 10 millones de usuarios puede reducirse a 10.000 supernodos después de la primera iteración completa, haciendo que las iteraciones posteriores sean muy rápidas.

**Implementación:** Se filtra el `VertexRDD` de tripletes para quedarse solo con aristas inter-comunidad, se agrupa por par de communityIds y se suman los pesos con `reduceByKey`. El resultado es un nuevo `Graph` más pequeño.

#### Modularidad Q

La modularidad mide la calidad de las comunidades detectadas. Formalmente:

```
Q = Σ_c [ L_c / m  −  (d_c / 2m)² ]
```

Donde `L_c` es la suma de pesos de aristas internas de la comunidad `c`, `d_c` es la suma de todos los grados de nodos en `c`, y `m` es el peso total de todas las aristas. Una comunidad con muchas aristas internas (alta `L_c`) y pocos grados hacia el exterior produce un Q alto. El máximo teórico es 1.0, valores prácticos buenos suelen estar entre 0.3 y 0.7.

Se calcula con un `aggregateMessages` que suma los pesos de aristas hacia cada vértice (grado ponderado), seguido de un `reduceByKey` que acumula grados por comunidad.

---

### ResultExporter — exportar resultados

**Archivo:** `ResultExporter.scala`

**Qué hace:** Une el `VertexRDD[communityId]` del algoritmo con el índice `idToName` para recuperar los `screen_names`, calcula el tamaño de cada comunidad y exporta dos archivos:

- `communities_raw/`: un archivo CSV con columnas `screen_name, community_id, community_size` (un registro por usuario).
- `stats/`: un CSV con una fila por comunidad, incluyendo tamaño y 5 miembros representativos.

También imprime en consola el ranking de las 20 comunidades más grandes.

---

## Conceptos clave de GraphX

| Concepto | Definición | Uso en este proyecto |
|---|---|---|
| `Graph[VD, ED]` | Grafo con atributos VD en vértices y ED en aristas | `Graph[Long, Double]`: communityId en vértices, peso en aristas |
| `VertexRDD[A]` | RDD especializado de pares (VertexId, A) colocado con el grafo | Resultado de `aggregateMessages` |
| `aggregateMessages` | Propaga mensajes a lo largo de aristas y los fusiona en vértices | Fase 1: calcular pesos por comunidad vecina; cálculo de modularidad |
| `joinVertices` | Actualiza atributos de vértices con un VertexRDD externo | Fase 1: actualizar communityId de cada nodo |
| `triplets` | RDD de tripletes (srcVertex, edge, dstVertex) | Fase 2: filtrar aristas inter-comunidad |
| `partitionBy` | Estrategia de distribución de aristas entre particiones | Reducir shuffle en `aggregateMessages` |

---

## Por qué Louvain y no otros algoritmos

Los algoritmos de detección de comunidades más comunes son:

| Algoritmo | Complejidad | Calidad | Disponible en GraphX |
|---|---|---|---|
| Label Propagation | O(E) | Media-baja | Sí (`LabelPropagation`) |
| Girvan-Newman | O(V·E²) | Alta | No |
| Infomap | O(E·log V) | Alta | No |
| **Louvain** | O(E·log V) aprox. | Alta | No (implementación propia) |
| Spectral clustering | O(V³) | Alta | No |

**Label Propagation** está disponible de serie en GraphX, pero tiende a producir comunidades de baja calidad (muchos nodos acaban en una sola comunidad gigante) y no tiene criterio de convergencia robusto.

**Louvain** ofrece la mejor relación calidad/coste: la fase de contracción hace que cada iteración sea más rápida que la anterior, y la optimización explícita de modularidad produce comunidades bien definidas. Es el algoritmo de facto para grafos sociales a gran escala.

---

## Instrucciones de ejecución (Windows + VSCode)

### Prerequisitos

1. **Java 11** — necesario para Spark. Descargar desde [Adoptium](https://adoptium.net/). Verificar: `java -version`
2. **Scala 2.12 + SBT** — instalar con [Coursier](https://get-coursier.io/docs/cli-installation): `cs install scala:2.12.18 sbt`
3. **Apache Spark 3.5** — descargar el binario pre-compilado de [spark.apache.org](https://spark.apache.org/downloads.html) y añadir `SPARK_HOME` al PATH
4. **winutils** — Spark en Windows necesita binarios Hadoop. Descargar `winutils.exe` para Hadoop 3.x de [cdarlint/winutils](https://github.com/cdarlint/winutils) y configurar `HADOOP_HOME`

### Variables de entorno (Windows)

```bat
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11..."
setx SPARK_HOME "C:\spark-3.5.1-bin-hadoop3"
setx HADOOP_HOME "C:\hadoop"
setx PATH "%PATH%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin"
```

### Paso 1 — Preparar datos

```bash
# Desde la raíz del proyecto
python prepare_data.py --input data/cache-0-json.gz --output data/tweets.jsonl

# Para probar con un subconjunto
python prepare_data.py --max 200000
```

### Paso 2 — Compilar el JAR

```bash
sbt assembly
```

Esto genera `target/scala-2.12/louvain-graphx-assembly-1.0.0.jar`.

### Paso 3 — Ejecutar

```bash
spark-submit ^
  --master local[*] ^
  --driver-memory 4g ^
  target/scala-2.12/louvain-graphx-assembly-1.0.0.jar ^
  --input  data/tweets.jsonl ^
  --output data/communities ^
  --min-weight 2.0 ^
  --max-iter 15
```

Ajustar `--driver-memory` según la RAM disponible. Para el dataset completo se recomiendan al menos 8 GB.

### Ejecución desde VSCode

Instalar la extensión **Metals** (Scala). Abre el proyecto y usa `Run` sobre `Main.scala` con las variables de entorno configuradas en `.vscode/launch.json`:

```json
{
  "configurations": [{
    "type": "scala",
    "name": "Louvain local",
    "mainClass": "louvain.Main",
    "args": ["--input", "data/tweets.jsonl", "--output", "data/communities"],
    "env": {
      "SPARK_LOCAL_HOSTNAME": "localhost"
    }
  }]
}
```

---

## Interpretación de resultados

Los archivos de salida en `data/communities/stats/` contienen una fila por comunidad con:

- `community_id`: identificador numérico de la comunidad
- `size`: número de usuarios en esa comunidad
- `sample_members`: 5 nombres de usuario representativos

Una distribución típica en grafos de Twitter muestra:
- Una o pocas comunidades muy grandes (temas trending, política)
- Muchas comunidades medianas (nichos temáticos: deporte, tecnología, entretenimiento)
- Numerosas comunidades pequeñas (grupos cerrados, conversaciones locales)

Un valor de modularidad Q > 0.4 indica una estructura de comunidades clara y bien separada.

---

## Limitaciones y posibles mejoras

**Heurística de movimiento en Fase 1.** La implementación actual elige la comunidad con mayor peso acumulado, en lugar del ΔQ exacto. El ΔQ exacto requeriría calcular los grados totales de comunidad en cada paso, añadiendo complejidad. En la práctica, la heurística de peso produce resultados similares con menor coste computacional.

**Grafo no dirigido.** Se pierde información de direccionalidad (quién inicia la interacción). Para análisis de influencia sería preferible un grafo dirigido con PageRank adicional.

**Una sola resolución.** Louvain produce una única partición a una resolución implícita determinada por la función de modularidad estándar. Para múltiples resoluciones (comunidades dentro de comunidades) se puede usar la variante Louvain con parámetro de resolución γ, modificando el cálculo de Q.

**Sin persistencia de jerarquía.** Este código sólo guarda el nivel final de comunidades. Guardar el estado en cada iteración permitiría explorar la estructura jerárquica completa que Louvain produce de forma natural.
