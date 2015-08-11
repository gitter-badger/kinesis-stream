name := "kinesis-stream"

version := "0.1.4"

organization := "com.localytics"

scalaVersion := "2.11.7"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

unmanagedSourceDirectories in Compile += baseDirectory.value / "src/examples/scala"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 ,"Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases"
 ,Resolver.sonatypeRepo("releases")
)

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"             % "7.1.3"
 ,"org.scalaz.stream" %% "scalaz-stream"           % "0.7.2a"
 ,"com.amazonaws"      % "amazon-kinesis-producer" % "0.9.0"
 ,"com.amazonaws"      % "amazon-kinesis-client"   % "1.5.1"
 ,"com.google.guava"   % "guava"                   % "18.0"
 ,"commons-io"         % "commons-io"              % "2.4"
 ,"org.scalacheck"    %% "scalacheck"              % "1.12.4"  % "test"
)

crossScalaVersions := Seq("2.10.5", "2.11.7") //, "2.12.0-M2")

scalacOptions ++= Seq(
  "-language:implicitConversions"
 ,"-language:higherKinds"
 ,"-deprecation"
 ,"-encoding", "UTF-8" // yes, this is 2 args
 ,"-feature"
 ,"-unchecked"
 ,"-Xfatal-warnings"
 ,"-Xlint"
 ,"-Ywarn-adapted-args"
 ,"-Ywarn-numeric-widen"
 ,"-Ywarn-value-discard"
 ,"-Xfuture"
)

val initCommands = """import scalaz._, Scalaz._, scalaz.stream._, scalaz.stream.Process._"""

initialCommands in console := initCommands 
initialCommands in consoleQuick := initCommands

// allow for ? instead of type lambdas. i.e Contravariant[Writer[?,O]]
// TODO: this isn't supported in some scala versions. Decide what to do.
//addCompilerPlugin("org.spire-math" % "kind-projector" % "0.6.2" cross CrossVersion.binary)

parallelExecution := false

parallelExecution in Test := true

bintrayOrganization := Some("localytics")

bintrayReleaseOnPublish in ThisBuild := false

bintrayPackageLabels := Seq("localytics", "scalaz-stream", "aws", "kinesis", "kpl", "kcl")
