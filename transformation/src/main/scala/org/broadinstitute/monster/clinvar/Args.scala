package org.broadinstitute.monster.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.ClinvarTransformationPipelineBuildInfo

@AppName("ClinVar transformation pipeline")
@AppVersion(ClinvarTransformationPipelineBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
case class Args(
  @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
  inputPrefix: String,
  @HelpMessage("Path where transformed ClinVar JSON should be written")
  outputPrefix: String
)
