name := "CSVTest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "1.2.1" % "provided").
    excludeAll( ExclusionRule( organization = "org.eclipse.jetty.orbit") ).
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")
)
