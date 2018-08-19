name := "jmh-spark"

version := "1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.8"

val JmhConfig = config("jmh")
resolvers += "scala-integration" at "https://scala-ci.typesafe.com/artifactory/scala-integration/"

lazy val benchmarks = addJmh(project).settings(
  mainClass in(Jmh, run) := Some("spark_benchmarks.MyRunner"),
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
)

lazy val addJavaOptions = javaOptions ++= {
  def getRef(version: String): String = {
    val HasSha = """.*(?:bin|pre)-([0-9a-f]{7,})(?:-.*)?""".r
    version match {
      case HasSha(sha) => sha
      case _ => "v" + version
    }
  }

  List(
    "-DscalaVersion=" + scalaVersion.value,
    "-DscalaRef=" + getRef(scalaVersion.value),
    "-Dsbt.launcher=" + (sys.props("java.class.path").split(java.io.File.pathSeparatorChar).find(_.contains("sbt-launch")).getOrElse(""))
  )
}

def addJmh(project: Project): Project = {
  project.enablePlugins(JmhPlugin).overrideConfigs(JmhConfig.extend(Compile))
}
