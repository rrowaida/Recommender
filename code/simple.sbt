name := "Simple project"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.4",
	"org.apache.spark" % "spark-mllib_2.11" % "2.4.4"
	)
