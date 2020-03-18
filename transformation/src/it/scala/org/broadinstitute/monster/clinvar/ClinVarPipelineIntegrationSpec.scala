package org.broadinstitute.monster.clinvar

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class ClinVarPipelineIntegrationSpec extends PipelineBuilderSpec[Args] {
  private val truthDir = File.currentWorkingDirectory / "src" / "it" / "test-files" / "outputs"
  private val compareDir = File.currentWorkingDirectory / "src" / "it" / "test-files" / "outputs-to-compare"
  private val inputDirString = s"${File.currentWorkingDirectory}/src/it/test-files/inputs"
  private val compareDirString = compareDir.pathAsString

  override val testArgs =
    Args(inputPrefix = inputDirString, outputPrefix = compareDirString)

  override val builder = ClinVarPipelineBuilder

  override def afterAll(): Unit = {
    File(compareDirString).delete()
    ()
  }

  /**
    *
    * Helper method to call the parsing method on the truth-files and the files-to-test.
    *
    * @param subDir The sub-directory of the outputs dir containing the files to read
    * @return A tuple of Set of Json, where the first one is the Set-to-test and the second one is the truth-Set
    */
  private def compareTruthAndCompSets(subDir: String): Unit = {
    it should s"have written the correct $subDir data" in {
      val expected = readMsgs(truthDir / subDir)
      val actual = readMsgs(compareDir / subDir)
      actual should contain theSameElementsAs expected
    }
  }

  behavior of "ClinVarPipeline"

  private val outputDirs = Set(
    "clinical_assertion",
    "clinical_assertion_observation",
    "clinical_assertion_trait",
    "clinical_assertion_trait_set",
    "clinical_assertion_variation",
    "gene",
    "gene_association",
    "rcv_accession",
    "submission",
    "submitter",
    "trait",
    "trait_mapping",
    "trait_set",
    "variation",
    "variation_archive"
  )

  outputDirs.foreach {
    it should behave like compareTruthAndCompSets(_)
  }
}
