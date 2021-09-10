name := "bitcoin-analyser"

version := "0.1"

scalaVersion := "2.12.14"
val sparkVersion = "3.0.2"

libraryDependencies ++= Seq(
  "org.lz4" % "lz4-java" % "1.8.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier ("tests"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier ("tests"),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
)
