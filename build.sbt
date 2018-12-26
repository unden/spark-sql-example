name := "spark-sql-example"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"
libraryDependencies += "org.spark-project.hive" % "hive-jdbc" % "1.2.1.spark2"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"
libraryDependencies += "org.apache.spark" %% "spark-yarn" % "2.1.0"

