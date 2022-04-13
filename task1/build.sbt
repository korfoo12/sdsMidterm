name := "FourHops"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies +=  "org.pegdown"    %  "pegdown"     % "1.6.0"  % "test"
scalacOptions += "-deprecation"