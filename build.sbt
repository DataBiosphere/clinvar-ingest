import org.broadinstitute.monster.sbt.model.JadeIdentifier

lazy val `clinvar-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_clinvar")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of NCBI's ClinVar archive, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.clinvar.jadeschema"
  )
