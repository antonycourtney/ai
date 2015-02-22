name := "CSVTest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "1.3.0-SNAPSHOT" % "provided").
    excludeAll( ExclusionRule( organization = "org.eclipse.jetty.orbit") ).
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog"),
  ("org.apache.spark" %% "spark-sql" % "1.3.0-SNAPSHOT" % "provided")    
)

resolvers += Resolver.mavenLocal
