lazy val app = (project in file(".")).settings(
  name := "instagraph",
  version := "1.0",
  scalaVersion := "2.11.12",
  mainClass in Compile := Some("com.instagraph.Instagraph")
)

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  "org.scalactic" %% "scalactic" % "3.1.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)