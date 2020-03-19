package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger

import org.broadinstitute.monster.clinvar.jadeschema.struct.InterpretationComment
import org.broadinstitute.monster.clinvar.Content
import org.broadinstitute.monster.clinvar.jadeschema.table._
import upack.Msg

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Wrapper for fully-parsed contents of a single submission to ClinVar.
  *
  * @param assertion info about the suspected impact of a variation
  * @param submitters organizations that submitted the assertion
  * @param submission provenance about the batch of assertions containing
  *                   the raw submission
  * @param variations raw variations observed by the submitter
  * @param traitSets groups of conditions observed by the submitter
  * @param traits conditions observed by the submitter
  * @param observations other information observed by the submitter
  */
case class ParsedScv(
  assertion: ClinicalAssertion,
  submitters: Array[Submitter],
  submission: Submission,
  variations: Array[ClinicalAssertionVariation],
  traitSets: Array[ClinicalAssertionTraitSet],
  traits: Array[ClinicalAssertionTrait],
  observations: Array[ClinicalAssertionObservation]
)

object ParsedScv {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Regex matching the YYYY-MM-DD portion of a date field which might also contain
    * a trailing timestamp.
    *
    * Used to normalize fields which are intended to be dates, not timestamps.
    */
  val SubmissionDatePattern: Regex = """^(\d{4}-\d{1,2}-\d{1,2}).*""".r

  /**
    * Convert a raw ClinicalAssertion payload into our model.
    *
    * @param variationId ID of the variation ClinVar has associated with the submission
    * @param vcvId ID of the archive containing the submission
    * @param referenceAccessions parsed RCVs from the archive
    * @param interpretation parsed data about the variation's interpreted effects
    * @param mappingsById links from submitted traits to curated traits, grouped by submission ID
    * @param rawAssertion payload to parse
    */
  def fromRawAssertion(
    variationId: String,
    vcvId: String,
    referenceAccessions: Array[RcvAccession],
    interpretation: ParsedInterpretation,
    mappingsById: Map[String, Array[TraitMapping]],
    rawAssertion: Msg
  ): ParsedScv = {
    // Extract submitter and submission data (easy).
    val submitter = extractSubmitter(rawAssertion.read[Msg]("ClinVarAccession"))
    val additionalSubmitters = rawAssertion
      .tryExtract[Array[Msg]]("AdditionalSubmitters", "SubmitterDescription")
      .fold(Array.empty[Submitter])(_.map(extractSubmitter))
    val submission = extractSubmission(submitter, additionalSubmitters, rawAssertion)

    // Extract the top-level set of traits described by the assertion.
    val assertionId = rawAssertion.extract[String]("@ID")
    val assertionAccession =
      rawAssertion.extract[String]("ClinVarAccession", "@Accession")
    val relevantTraitMappings = mappingsById.getOrElse(assertionId, Array.empty)

    val directTraitSet = ParsedScvTraitSet.fromRawSetWrapper(
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    )

    // Extract info about any other traits observed in the submission.
    val (observations, observedTraitSets) = extractObservations(
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    ).unzip

    // Flatten out nested trait-set info.
    val (allTraitSets, allTraits) = {
      val directSetAsArray = directTraitSet.toArray
      val actualObservedSets = observedTraitSets.flatten
      val sets = (directSetAsArray ++ actualObservedSets).map(_.traitSet)
      val traits = (directSetAsArray ++ actualObservedSets).flatMap(_.traits)
      (sets, traits)
    }

    // Extract the tree of variation records stored in the submission.
    val variations = extractVariations(assertionAccession, rawAssertion)

    // Extract remaining top-level info about the submitted assertion.
    val assertion = {
      val referenceTraitSetId = directTraitSet.flatMap { traitSet =>
        interpretation.traitSets
          .find(_.traitIds.sameElements(traitSet.traits.flatMap(_.traitId)))
          .map(_.id)
      }
      val relatedRcv = referenceAccessions.find { rcv =>
        rcv.traitSetId.isDefined && rcv.traitSetId == referenceTraitSetId
      }

      ClinicalAssertion(
        id = assertionAccession,
        version = rawAssertion.extract[Long]("ClinVarAccession", "@Version"),
        internalId = assertionId,
        variationArchiveId = vcvId,
        variationId = variationId,
        submitterId = submitter.id,
        submissionId = submission.id,
        rcvAccessionId = relatedRcv.map(_.id),
        traitSetId = referenceTraitSetId,
        clinicalAssertionTraitSetId = directTraitSet.map(_.traitSet.id),
        clinicalAssertionObservationIds = observations.map(_.id),
        title = rawAssertion.tryExtract[String]("ClinVarSubmissionID", "@title"),
        localKey = rawAssertion.tryExtract[String]("ClinVarSubmissionID", "@localKey"),
        assertionType = rawAssertion.tryExtract[String]("Assertion", "$"),
        dateCreated = rawAssertion.tryExtract[LocalDate]("@DateCreated"),
        dateLastUpdated = rawAssertion.tryExtract[LocalDate]("@DateLastUpdated"),
        submittedAssembly =
          rawAssertion.tryExtract[String]("ClinVarSubmissionID", "@submittedAssembly"),
        recordStatus = rawAssertion.tryExtract[String]("RecordStatus", "$"),
        reviewStatus = rawAssertion.tryExtract[String]("ReviewStatus", "$"),
        interpretationDescription =
          rawAssertion.tryExtract[String]("Interpretation", "Description", "$"),
        interpretationDateLastEvaluated = rawAssertion
          .tryExtract[String]("Interpretation", "@DateLastEvaluated")
          .flatMap {
            case SubmissionDatePattern(trimmed) => Some(LocalDate.parse(trimmed))
            case _                              => None
          },
        interpretationComments = rawAssertion
          .tryExtract[Array[Msg]]("Interpretation", "Comment")
          .getOrElse(Array.empty)
          .map { comment =>
            InterpretationComment(
              `type` = comment.tryExtract[String]("@Type"),
              text = comment.extract[String]("$")
            )
          },
        content = Content.encode(rawAssertion)
      )
    }

    ParsedScv(
      assertion = assertion,
      submitters = submitter +: additionalSubmitters,
      submission = submission,
      variations = variations,
      observations = observations,
      traitSets = allTraitSets,
      traits = allTraits
    )
  }

  /** Extract submitter fields from a raw ClinicalAssertion payload. */
  private def extractSubmitter(rawAssertion: Msg): Submitter = Submitter(
    id = rawAssertion.extract[String]("@OrgID"),
    submitterName = rawAssertion.tryExtract[String]("@SubmitterName"),
    orgCategory = rawAssertion.tryExtract[String]("@OrganizationCategory"),
    orgAbbrev = rawAssertion.tryExtract[String]("@OrgAbbreviation")
  )

  /** Extract submission fields from a raw ClinicalAssertion payload. */
  private def extractSubmission(
    submitter: Submitter,
    additionalSubmitters: Array[Submitter],
    rawAssertion: Msg
  ): Submission = {
    val date = rawAssertion.extract[LocalDate]("@SubmissionDate")

    Submission(
      id = s"${submitter.id}.$date",
      submitterId = submitter.id,
      additionalSubmitterIds = additionalSubmitters.map(_.id),
      submissionDate = date,
      submissionNames = rawAssertion
        .tryExtract[Array[Msg]]("SubmissionNameList", "SubmissionName")
        .getOrElse(Array.empty)
        .map(_.extract[String]("$"))
    )
  }

  /**
    * Extract observation data from a raw clinical assertion.
    *
    * @param assertionAccession ID of the assertion
    * @param rawAssertion raw assertion payload to extract from
    */
  private def extractObservations(
    assertionAccession: String,
    rawAssertion: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): Array[(ClinicalAssertionObservation, Option[ParsedScvTraitSet])] = {
    val observationCounter = new AtomicInteger(0)
    rawAssertion
      .tryExtract[Array[Msg]]("ObservedInList", "ObservedIn")
      .getOrElse(Array.empty)
      .map { rawObservation =>
        val observationId =
          s"$assertionAccession.${observationCounter.getAndIncrement()}"
        val parsedSet = ParsedScvTraitSet.fromRawSetWrapper(
          observationId,
          rawObservation,
          referenceTraits,
          traitMappings
        )
        val observation = ClinicalAssertionObservation(
          id = observationId,
          clinicalAssertionTraitSetId = parsedSet.map(_.traitSet.id),
          content = Content.encode(rawObservation)
        )

        (observation, parsedSet)
      }
  }

  /**
    * Extract variation data from a raw clinical assertion.
    *
    * @param assertionAccession ID of the assertion
    * @param rawAssertion raw assertion payload to extract from
    */
  private def extractVariations(
    assertionAccession: String,
    rawAssertion: Msg
  ): Array[ClinicalAssertionVariation] = {
    val buffer = new mutable.ArrayBuffer[ClinicalAssertionVariation]()
    val counter = new AtomicInteger(0)

    // Traverse the tree of SCV variations, parsing each one and adding it to a buffer.
    def extractAndAccumulateDescendants(variationWrapper: Msg): VariationDescendants =
      VariationDescendants.fromVariationWrapper(variationWrapper) {
        (subtype, rawVariation) =>
          val baseVariation = ClinicalAssertionVariation(
            id = s"$assertionAccession.${counter.getAndIncrement()}",
            clinicalAssertionId = assertionAccession,
            subclassType = subtype,
            childIds = Array.empty,
            descendantIds = Array.empty,
            variationType = rawVariation
              .tryExtract[String]("VariantType", "$")
              .orElse(rawVariation.tryExtract[String]("VariationType", "$")),
            // NOTE: Left `None` here on purpose; it gets filled in later once the
            // child variations have been extracted out.
            content = None
          )
          val descendants = extractAndAccumulateDescendants(rawVariation)
          val allAncestry = descendants.childIds ::: descendants.descendantIds
          buffer.append {
            baseVariation.copy(
              childIds = descendants.childIds.toArray,
              descendantIds = allAncestry.toArray,
              content = Content.encode(rawVariation)
            )
          }
          (baseVariation.id, allAncestry)
      }

    // Build up the buffer of variations while traversing the tree.
    extractAndAccumulateDescendants(rawAssertion)
    // Return whatever was buffered.
    buffer.toArray
  }
}
