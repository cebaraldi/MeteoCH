name := "MeteoCH"

version := "0.1"

scalaVersion := "2.12.14"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // UTF-8, ISO-8859-1
  "-feature",
  "-unchecked"
)

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)

//// Fork a new JVM for 'run' and 'test:run', to avoid JavaFX double initialization problems
//fork := true
//
//// Determine OS version of JavaFX binaries
//lazy val osName = System.getProperty("os.name") match {
//  case n if n.startsWith("Linux") => "linux"
//  case n if n.startsWith("Mac") => "mac"
//  case n if n.startsWith("Windows") => "win"
//  case _ => throw new Exception("Unknown platform!")
//}
//
//// Add JavaFX dependencies
//lazy val javaFXModules = Seq("base", "controls", "fxml", "graphics", "media", "swing", "web")
//libraryDependencies ++= javaFXModules.map(m =>
//  "org.openjfx" % s"javafx-$m" % "15.0.1" classifier osName
//)