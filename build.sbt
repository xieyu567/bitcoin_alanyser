name := "bitcoin-analyser"

version := "0.1"

scalaVersion := "2.12.14"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.lz4" % "lz4-java" % "1.8.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.typelevel" %% "cats-core" % "2.6.1",
  "org.typelevel" %% "cats-effect" % "3.2.7",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided exclude("net.jpountz.lz4", "lz4"),
  "com.pusher" % "pusher-java-client" % "2.2.6"
)

scalacOptions += "-Ypartial-unification"

// Avoids SI-3623
//target := file("/tmp/sbt/bitcoin-analyser")
