package org.broadinstitute.monster.clinvar

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// IntelliJ lies, this is actually used implicitly.
import Args.dateParser

/** Entry-point for the ClinVar pipeline's Docker image. */
object ClinVarPipeline extends ScioApp[Args] {
  override def pipelineBuilder: PipelineBuilder[Args] = ClinVarPipelineBuilder
}
