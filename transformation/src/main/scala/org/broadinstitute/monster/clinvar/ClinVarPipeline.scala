package org.broadinstitute.monster.clinvar

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the ClinVar pipeline's Docker image. */
object ClinVarPipeline extends ScioApp[Args] {
  override def pipelineBuilder: PipelineBuilder[Args] = ClinVarPipelineBuilder
}
