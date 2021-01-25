import _root_.io.circe.Json

lazy val `clinvar-ingest` = project
  .in(file("."))
  .aggregate(`clinvar-schema`, `clinvar-transformation-pipeline`, `clinvar-orchestration-workflow`)
  .settings(publish / skip := true)

lazy val `clinvar-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.clinvar.jadeschema.table",
    jadeTableFragmentPackage := "org.broadinstitute.monster.clinvar.jadeschema.fragment",
    jadeStructPackage := "org.broadinstitute.monster.clinvar.jadeschema.struct"
  )

lazy val `clinvar-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`clinvar-schema`)
  .settings(
    dockerBaseImage := "ghcr.io/graalvm/graalvm-ce:ol7-java8-20.3.1"
  )

lazy val `clinvar-orchestration-workflow` = project
  .in(file("orchestration"))
  .enablePlugins(MonsterHelmPlugin)
  .settings(
    helmChartOrganization := "DataBiosphere",
    helmChartRepository := "clinvar-ingest",
    helmInjectVersionValues := { (baseValues, version) =>
      val jsonVersion = Json.fromString(version)
      val schemaVersionValues = Json.obj(
        "version" -> jsonVersion,
        "argoTemplates" -> Json.obj(
          "diffBQTable" -> Json.obj(
            "schemaImageVersion" -> jsonVersion
          )
        )
      )
      baseValues.deepMerge(schemaVersionValues)
    }
  )
