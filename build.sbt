import org.broadinstitute.monster.sbt.model.JadeIdentifier

val beamVersion = "2.16.0"
val betterFilesVersion = "3.8.0"
val logbackVersion = "1.2.3"
val scioVersion = "0.8.0"
val uPickleVersion = "0.8.0"

val scalatestVersion = "3.1.0"

lazy val `clinvar-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_clinvar")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of NCBI's ClinVar archive, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.clinvar.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.clinvar.jadeschema.struct",
    scalacOptions ++= Seq(
      "-Xmacro-settings:show-coder-fallback=true",
      "-language:higherKinds"
    ),
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.lihaoyi" %% "upickle" % uPickleVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "io.circe" %% "circe-parser" % MonsterJadeDatasetPlugin.CirceVersion
    ),
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
    ).map(_ % Runtime),
    libraryDependencies ++= Seq(
      "com.github.pathikrit" %% "better-files" % betterFilesVersion,
      "com.spotify" %% "scio-test" % scioVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}")
  )
