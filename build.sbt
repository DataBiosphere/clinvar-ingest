import _root_.io.circe.Json
import org.broadinstitute.monster.sbt.model.JadeIdentifier

lazy val `clinvar-ingest` = project
  .in(file("."))
  .aggregate(`clinvar-schema`, `clinvar-transformation-pipeline`)
  .settings(publish / skip := true)

lazy val `clinvar-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_clinvar")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of NCBI's ClinVar archive, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.clinvar.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.clinvar.jadeschema.struct"
  )

lazy val `clinvar-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`clinvar-schema`)

lazy val `clinvar-orchestration-workflow` = project
  .in(file("orchestration"))
  .enablePlugins(MonsterHelmPlugin)
  .settings(
    helmChartOrganization := "DataBiosphere",
    helmChartRepository := "clinvar-ingest",
    helmInjectVersionValues := { (baseValues, version) =>
      val schemaVersionValues = Json.obj(
        "argoTemplates" -> Json.obj(
          "diffBQTable" -> Json.obj(
            "schemaImageVersion" -> Json.fromString(version)
          )
        )
      )
      baseValues.deepMerge(schemaVersionValues)
    }
  )
