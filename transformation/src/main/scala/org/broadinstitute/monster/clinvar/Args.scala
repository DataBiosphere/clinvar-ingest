package org.broadinstitute.monster.clinvar

import java.time.LocalDate

import caseapp.core.argparser.ArgParser
import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.buildinfo.ClinvarTransformationPipelineBuildInfo

@AppName("ClinVar transformation pipeline")
@AppVersion(ClinvarTransformationPipelineBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
case class Args(
  @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
  inputPrefix: String,
  @HelpMessage("Release date of the ClinVar archive being processed")
  releaseDate: LocalDate,
  @HelpMessage("Path where transformed ClinVar JSON should be written")
  outputPrefix: String
)

object Args {

  implicit val dateParser: ArgParser[LocalDate] = ArgParser.string.xmap(
    _.toString,
    LocalDate.parse(_)
  )
}
