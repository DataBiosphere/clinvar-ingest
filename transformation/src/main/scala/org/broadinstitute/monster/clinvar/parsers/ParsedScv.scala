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
  submitters: List[Submitter],
  submission: Submission,
  variations: List[ClinicalAssertionVariation],
  traitSets: List[ClinicalAssertionTraitSet],
  traits: List[ClinicalAssertionTrait],
  observations: List[ClinicalAssertionObservation]
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
    releaseDate: LocalDate,
    variationId: String,
    vcvId: String,
    referenceAccessions: List[RcvAccession],
    interpretation: ParsedInterpretation,
    mappingsById: Map[String, List[TraitMapping]],
    rawAssertion: Msg
  ): ParsedScv = {
    // Extract submitter and submission data (easy).
    val rawAccession = rawAssertion.read[Msg]("ClinVarAccession")

    val submitter = extractSubmitter(releaseDate, rawAccession)
    val additionalSubmitters = rawAssertion
      .tryExtract[List[Msg]]("AdditionalSubmitters", "SubmitterDescription")
      .fold(List.empty[Submitter])(_.map(extractSubmitter(releaseDate, _)))
    val submission = extractSubmission(submitter, additionalSubmitters, rawAssertion)

    // Extract the top-level set of traits described by the assertion.
    val assertionId = rawAssertion.extract[String]("@ID")
    val assertionAccession = rawAccession.extract[String]("@Accession")
    val relevantTraitMappings = mappingsById.getOrElse(assertionId, List.empty)

    val directTraitSet = ParsedScvTraitSet.fromRawSetWrapper(
      releaseDate,
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    )

    // Extract info about any other traits observed in the submission.
    val (observations, observedTraitSets) = extractObservations(
      releaseDate,
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    ).unzip

    // Flatten out nested trait-set info.
    val (allTraitSets, allTraits) = {
      val directSetAsList = directTraitSet.toList
      val actualObservedSets = observedTraitSets.flatten
      val sets = (directSetAsList ++ actualObservedSets).map(_.traitSet)
      val traits = (directSetAsList ++ actualObservedSets).flatMap(_.traits)
      (sets, traits)
    }

    // Extract the tree of variation records stored in the submission.
    val variations = extractVariations(releaseDate, assertionAccession, rawAssertion)

    // Extract remaining top-level info about the submitted assertion.
    val assertion = {
      val referenceTraitSetId = interpretation.traitSets match {
        case Nil           => None
        case single :: Nil => Some(single.id)
        case many =>
          directTraitSet.flatMap { traitSet =>
            many
              .find(_.traitIds == traitSet.traits.flatMap(_.traitId))
              .map(_.id)
          }
      }
      val relatedRcv = referenceAccessions.find { rcv =>
        rcv.traitSetId.isDefined && rcv.traitSetId == referenceTraitSetId
      }

      ClinicalAssertion(
        id = assertionAccession,
        releaseDate = releaseDate,
        version = rawAccession.extract[Long]("@Version"),
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
          .tryExtract[List[Msg]]("Interpretation", "Comment")
          .getOrElse(List.empty)
          .map { comment =>
            InterpretationComment(
              `type` = comment.tryExtract[String]("@Type"),
              text = comment.extract[String]("$")
            )
          },
        submissionNames = rawAssertion
          .tryExtract[List[Msg]]("SubmissionNameList", "SubmissionName")
          .getOrElse(List.empty)
          .map(_.extract[String]("$")),
        content = {
          // Pop out the accession type to reduce noise in our unmodeled content column.
          val _ = rawAssertion.tryExtract[Msg]("ClinVarAccession", "@Type")
          Content.encode(rawAssertion)
        }
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
  private def extractSubmitter(releaseDate: LocalDate, rawAssertion: Msg): Submitter = {
    val name = rawAssertion.tryExtract[String]("@SubmitterName")
    val abbrev = rawAssertion.tryExtract[String]("@OrgAbbreviation")

    // NOTE: At this level the 'current' & 'all' fields are always equal.
    // They diverge at a higher level in the code that aggregates across
    // submitter records in a release.
    Submitter(
      id = rawAssertion.extract[String]("@OrgID"),
      releaseDate = releaseDate,
      orgCategory = rawAssertion.tryExtract[String]("@OrganizationCategory"),
      currentName = name,
      allNames = name.toList,
      currentAbbrev = abbrev,
      allAbbrevs = abbrev.toList
    )
  }

  /** Extract submission fields from a raw ClinicalAssertion payload. */
  private def extractSubmission(
    submitter: Submitter,
    additionalSubmitters: List[Submitter],
    rawAssertion: Msg
  ): Submission = {
    val date = rawAssertion.extract[LocalDate]("@SubmissionDate")

    Submission(
      id = s"${submitter.id}.$date",
      releaseDate = submitter.releaseDate,
      submitterId = submitter.id,
      additionalSubmitterIds = additionalSubmitters.map(_.id),
      submissionDate = date
    )
  }

  /**
    * Extract observation data from a raw clinical assertion.
    *
    * @param assertionAccession ID of the assertion
    * @param rawAssertion raw assertion payload to extract from
    */
  private def extractObservations(
    releaseDate: LocalDate,
    assertionAccession: String,
    rawAssertion: Msg,
    referenceTraits: List[Trait],
    traitMappings: List[TraitMapping]
  ): List[(ClinicalAssertionObservation, Option[ParsedScvTraitSet])] = {
    val observationCounter = new AtomicInteger(0)
    rawAssertion
      .tryExtract[List[Msg]]("ObservedInList", "ObservedIn")
      .getOrElse(List.empty)
      .map { rawObservation =>
        val observationId =
          s"$assertionAccession.${observationCounter.getAndIncrement()}"
        val parsedSet = ParsedScvTraitSet.fromRawSetWrapper(
          releaseDate,
          observationId,
          rawObservation,
          referenceTraits,
          traitMappings
        )
        val observation = ClinicalAssertionObservation(
          id = observationId,
          releaseDate = releaseDate,
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
    releaseDate: LocalDate,
    assertionAccession: String,
    rawAssertion: Msg
  ): List[ClinicalAssertionVariation] = {
    val buffer = new mutable.ListBuffer[ClinicalAssertionVariation]()
    val counter = new AtomicInteger(0)

    // Traverse the tree of SCV variations, parsing each one and adding it to a buffer.
    def extractAndAccumulateDescendants(variationWrapper: Msg): VariationDescendants =
      VariationDescendants.fromVariationWrapper(variationWrapper) { (subtype, rawVariation) =>
        val baseVariation = ClinicalAssertionVariation(
          id = s"$assertionAccession.${counter.getAndIncrement()}",
          releaseDate = releaseDate,
          clinicalAssertionId = assertionAccession,
          subclassType = subtype,
          childIds = List.empty,
          descendantIds = List.empty,
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
            childIds = descendants.childIds,
            descendantIds = allAncestry,
            content = Content.encode(rawVariation)
          )
        }
        (baseVariation.id, allAncestry)
      }

    // Build up the buffer of variations while traversing the tree.
    extractAndAccumulateDescendants(rawAssertion)
    // Return whatever was buffered.
    buffer.toList
  }
}
