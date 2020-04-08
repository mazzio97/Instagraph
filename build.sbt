lazy val app = (project in file(".")).
  settings(
    name := "instagraph",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.instagraph.Instagraph")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-graphx" % "2.4.5" % "provided",
  "org.scalactic" %% "scalactic" % "3.1.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)