package org.broadinstitute.monster.clinvar

import java.time.LocalDate

import better.files.File
import com.spotify.scio.testing.PipelineSpec
import io.circe.Json
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class ClinVarPipelineIntegrationSpec
    extends PipelineSpec
    with Matchers
    with BeforeAndAfterAll {
  behavior of "ClinVarPipeline"

  private val truthDir = File.currentWorkingDirectory / "src" / "it" / "test-files" / "outputs"
  private val compareDir = File.currentWorkingDirectory / "src" / "it" / "test-files" / "outputs-to-compare"
  private val inputDirString = s"${File.currentWorkingDirectory}/src/it/test-files/inputs"
  private val compareDirString = compareDir.pathAsString

  override def beforeAll(): Unit = {
    runWithRealContext(PipelineOptionsFactory.create()) { sc =>
      ClinVarPipeline.buildPipeline(
        sc,
        ClinVarPipeline.Args(
          inputPrefix = inputDirString,
          releaseDate = LocalDate.parse("2020-02-18"),
          archiveDrsId = "fake-drs-id",
          outputPrefix = compareDirString
        )
      )
    }.waitUntilDone()
    ()
  }

  override def afterAll(): Unit = {
    File(compareDirString).delete()
    ()
  }

  /**
    *
    * Helper method to parse the output files into a comparable format.
    *
    * @param directory The path to the directory where the files live.
    * @param filePattern The glob pattern of files to read.
    * @return One Set of Json that has every json object written to the output files.
    */
  private def createSetFromFiles(directory: File, filePattern: String): Set[Json] = {
    directory
      .glob(filePattern)
      .flatMap(_.lineIterator)
      .map { line =>
        val maybeParsed = io.circe.parser.parse(line)
        maybeParsed.fold(
          err => throw new Exception(s"Failed to parse input line as JSON: $line", err),
          identity
        )
      }
      .toSet
  }

  /**
    *
    * Helper method to call the parsing method on the truth-files and the files-to-test.
    *
    * @param filePattern The glob pattern of files to read.
    * @return A tuple of Set of Json, where the first one is the Set-to-test and the second one is the truth-Set.
    */
  private def compareTruthAndCompSets(filePattern: String, description: String): Unit = {
    it should description in {
      val expected = createSetFromFiles(truthDir, filePattern)
      val actual = createSetFromFiles(compareDir, filePattern)
      actual should contain theSameElementsAs expected
    }
  }

  private val filePatternsAndDescriptions = Set(
    ("clinical_assertion/*.json", "have written the correct clinical_assertion data"),
    (
      "clinical_assertion_observation/*.json",
      "have written the correct clinical_assertion_observation data"
    ),
    (
      "clinical_assertion_trait/*.json",
      "have written the correct clinical_assertion_trait data"
    ),
    (
      "clinical_assertion_trait_set/*.json",
      "have written the correct clinical_assertion_trait_set data"
    ),
    (
      "clinical_assertion_variation/*.json",
      "have written the correct clinical_assertion_variation data"
    ),
    ("gene/*.json", "have written the correct gene data"),
    ("gene_association/*.json", "have written the correct gene_association data"),
    ("rcv_accession/*.json", "have written the correct rcv_accession data"),
    ("submission/*.json", "have written the correct submission data"),
    ("submitter/*.json", "have written the correct submitter data"),
    ("trait/*.json", "have written the correct trait data"),
    ("trait_mapping/*.json", "have written the correct trait_mapping data"),
    ("trait_set/*.json", "have written the correct trait_set data"),
    ("variation/*.json", "have written the correct variation data"),
    ("variation_archive/*.json", "have written the correct variation_archive data"),
    (
      "variation_archive_release/*.json",
      "have written the correct variation_archive_release data"
    )
  )

  filePatternsAndDescriptions.foreach {
    case (filePattern, description) =>
      it should behave like compareTruthAndCompSets(filePattern, description)
  }
}
