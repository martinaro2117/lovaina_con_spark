name         := "louvain-graphx"
version      := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"     % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx"  % sparkVersion % "provided"
)

// Fat JAR para spark-submit en local
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// Para ejecutar con spark-submit local sin cluster
run / fork              := true
run / javaOptions       ++= Seq(
  "-Dspark.master=local[*]",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
