val simpleBench = (project in file("simpleBench"))
.settings(
    name := "simpleBench",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.1.4"
)

val queryAnalysis = (project in file("queryAnalysis"))
  .settings(
    name := "queryAnalysis",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
    libraryDependencies += "com.github.haifengl" % "smile-scala_2.11" % "1.3.1",
      libraryDependencies += "com.github.jsqlparser" % "jsqlparser" % "1.1"
  )

val root = (project in file("."))
    .aggregate(queryAnalysis)

