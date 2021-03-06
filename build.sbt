import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

name := "test"

version := "1.0"

scalaVersion := "2.10.4"

val scalaTestVersion = "2.0"

val akkaVersion = "2.3.3"

//val akkaVersion = "2.4-SNAPSHOT"

//resolvers += "coinport-repo" at "http://192.168.0.105:8081/nexus/content/groups/public"

resolvers += "maven2" at "http://repo1.maven.org/maven2"

libraryDependencies += ("com.coinport" %% "akka-persistence-hbase" % "1.0.15-SNAPSHOT")
  .exclude("org.jboss.netty", "netty")
  .exclude("org.jruby", "jruby-complete")
  .exclude("javax.xml.stream", "stax-api")
  .exclude("javax.xml.stream", "stax-api")
  .exclude("commons-beanutils", "commons-beanutils")
  .exclude("commons-beanutils", "commons-beanutils-core")
  .exclude("tomcat", "jasper-runtime")
  .exclude("tomcat", "jasper-compiler")
  .exclude("org.slf4j", "slf4j-log4j12")
  .exclude("org.jboss.netty", "netty")
  .exclude("org.mortbay.jetty", "jsp-api-2.1")
  .exclude("org.mortbay.jetty", "servlet-api")
  .exclude("org.mortbay.jetty", "servlet-api-2.5")
  .exclude("javax.servlet", "jsp-api")
  .exclude("javax.servlet", "servlet-api")
  .exclude("stax", "stax-api")
  .exclude("commons-logging", "commons-logging")
  .exclude("org.apache.hadoop", "hadoop-yarn-api")
//  .exclude("commons-beanutils", "commons-beanutils")
//  .exclude("commons-beanutils", "commons-beanutils-core")
//  .exclude("tomcat", "jasper-runtime")
//  .exclude("tomcat", "jasper-compiler")

//libraryDependencies += ("com.github.scullxbones" % "akka-persistence-mongo-casbah_2.10" % "0.0.4-SNAPSHOT")
  //.exclude("joda-time", "joda-time")

libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12" % "1.6.0",
    "commons-logging" % "commons-logging" % "1.2",
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit"   % akkaVersion,
    "org.scalatest"              %% "scalatest"                    % scalaTestVersion,
    "junit"                       % "junit"                        % "4.10",
    "org.iq80.leveldb"            % "leveldb"                      % "0.5",
    "org.fusesource.leveldbjni"   % "leveldbjni-all"               % "1.7",
    "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.42" % "test",
    "com.twitter" %% "util-eval" % "6.12.1"
    // "com.coinport" %% "coinex-client" % "1.1.32-SNAPSHOT"
    )

libraryDependencies += ("org.hbase"        % "asynchbase"    % "1.8.0")
.exclude("org.slf4j", "log4j-over-slf4j")
.exclude("org.slf4j", "jcl-over-slf4j")

sbtassembly.Plugin.assemblySettings ++ Seq(sbtassembly.Plugin.assemblySettings: _*)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "pom.xml" => MergeStrategy.first
    case x => old(x)
  }
}

//mergeStrategy in assembly := MergeStrategy.first

// dependencyGraph.net.virtualvoid.sbt.graph.Plugin.graphSettings: _*

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

net.virtualvoid.sbt.graph.Plugin.graphSettings

parallelExecution in Test := false

// publishing settings

// test in assembly := {}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

publishTo <<= (version) { version: String =>
  val nexus = "http://192.168.0.105:8081/nexus/content/repositories/"
    if (version.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "snapshots/")
    else                                   
      Some("releases"  at nexus + "releases/")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
