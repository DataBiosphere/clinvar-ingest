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
  * @param submitter organization that submitted the assertion
  * @param submission provenance about the batch of assertions containing
  *                   the raw submission
  * @param variations raw variations observed by the submitter
  * @param traitSets groups of conditions observed by the submitter
  * @param traits conditions observed by the submitter
  * @param observations other information observed by the submitter
  */
case class ParsedScv(
  assertion: ClinicalAssertion,
  submitter: Submitter,
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
    val submitter = extractSubmitter(rawAssertion)
    val submission = extractSubmission(submitter.id, rawAssertion)

    // Extract the top-level set of traits described by the assertion.
    val assertionId = rawAssertion.extract[String]("@ID")
    val assertionAccession =
      rawAssertion.extract[String]("ClinVarAccession", "@Accession")
    val relevantTraitMappings = mappingsById.getOrElse(assertionId, Array.empty)

    val (directTraitSet, directTraits) = extractTraitSet(
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    )

    // Extract info about any other traits observed in the submission.
    val (observations, observedTraitSets, observedTraits) = extractObservations(
      assertionAccession,
      rawAssertion,
      interpretation.traits,
      relevantTraitMappings
    )

    // Extract the tree of variation records stored in the submission.
    val variations = extractVariations(assertionAccession, rawAssertion)

    // Extract remaining top-level info about the submitted assertion.
    val assertion = {
      val referenceTraitSetId = interpretation.traitSets
        .find(_.traitIds.sameElements(directTraits.flatMap(_.traitId)))
        .map(_.id)
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
        clinicalAssertionTraitSetId = directTraitSet.map(_.id),
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
      submitter = submitter,
      submission = submission,
      variations = variations,
      observations = observations,
      traitSets = directTraitSet.toArray ++ observedTraitSets,
      traits = directTraits ++ observedTraits
    )
  }

  /** Extract submitter fields from a raw ClinicalAssertion payload. */
  private def extractSubmitter(rawAssertion: Msg): Submitter = Submitter(
    id = rawAssertion.extract[String]("ClinVarAccession", "@OrgID"),
    submitterName = rawAssertion.tryExtract[String]("ClinVarAccession", "@SubmitterName"),
    orgCategory =
      rawAssertion.tryExtract[String]("ClinVarAccession", "@OrganizationCategory"),
    orgAbbrev = rawAssertion.tryExtract[String]("ClinVarAccession", "@OrgAbbreviation")
  )

  /** Extract submission fields from a raw ClinicalAssertion payload. */
  private def extractSubmission(submitterId: String, rawAssertion: Msg): Submission = {
    val date = rawAssertion.extract[LocalDate]("@SubmissionDate")
    val additionalSubmitters = rawAssertion
      .tryExtract[Array[Msg]]("AdditionalSubmitters", "SubmitterDescription")
      .getOrElse(Array.empty)

    Submission(
      id = s"$submitterId.$date",
      submitterId = submitterId,
      additionalSubmitterIds = additionalSubmitters.map(_.extract[String]("@OrgID")),
      submissionDate = date,
      submissionNames = rawAssertion
        .tryExtract[Array[Msg]]("SubmissionNameList", "SubmissionName")
        .getOrElse(Array.empty)
        .map(_.extract[String]("$"))
    )
  }

  /**
    * TODO
    *
    * @param assertionAccession TODO
    * @param rawAssertion TODO
    */
  private def extractObservations(
    assertionAccession: String,
    rawAssertion: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): (
    Array[ClinicalAssertionObservation],
    Array[ClinicalAssertionTraitSet],
    Array[ClinicalAssertionTrait]
  ) = {
    val observationCounter = new AtomicInteger(0)
    val zero = (
      Array.empty[ClinicalAssertionObservation],
      Array.empty[ClinicalAssertionTraitSet],
      Array.empty[ClinicalAssertionTrait]
    )
    rawAssertion
      .tryExtract[Array[Msg]]("ObservedInList", "ObservedIn")
      .getOrElse(Array.empty)
      .foldLeft(zero) {
        case ((observationAcc, traitSetAcc, traitAcc), rawObservation) =>
          val observationId =
            s"$assertionAccession.${observationCounter.getAndIncrement()}"
          val (observedTraitSet, observedTraits) = extractTraitSet(
            observationId,
            rawObservation,
            referenceTraits,
            traitMappings
          )
          val observation = ClinicalAssertionObservation(
            id = observationId,
            clinicalAssertionTraitSetId = observedTraitSet.map(_.id),
            content = Content.encode(rawObservation)
          )

          (
            observation +: observationAcc,
            observedTraitSet.toArray ++ traitSetAcc,
            observedTraits ++ traitAcc
          )
      }
  }

  /**
    * TODO
    *
    * @param setId ID to assign to the extracted set
    * @param setWrapper TODO
    * @param referenceTraits curated traits from the same archive as the payload
    * @param traitMappings mappings from raw traits to curated traits
    */
  private def extractTraitSet(
    setId: String,
    setWrapper: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): (Option[ClinicalAssertionTraitSet], Array[ClinicalAssertionTrait]) =
    setWrapper.tryExtract[Msg]("TraitSet") match {
      case None => (None, Array.empty)
      case Some(rawSet) =>
        val counter = new AtomicInteger(0)
        val traits = rawSet
          .extract[Array[Msg]]("Trait")
          .map { rawTrait =>
            val metadata = TraitMetadata.fromRawTrait(rawTrait) { _ =>
              // No meaningful ID for these nested traits.
              s"$setId.${counter.getAndIncrement()}"
            }
            val matchingTrait =
              findMatchingTrait(metadata, referenceTraits, traitMappings)

            ClinicalAssertionTrait(
              id = metadata.id,
              traitId = matchingTrait.map(_.id),
              `type` = metadata.`type`,
              name = metadata.name,
              alternateNames = metadata.alternateNames,
              medgenId = metadata.medgenId.orElse(matchingTrait.flatMap(_.medgenId)),
              xrefs = metadata.xrefs,
              // NOTE: This must always be the last filled-in field, so that every
              // other field is popped from the raw payload before it's bundled into
              // the content column.
              content = Content.encode(rawTrait)
            )
          }
        val traitSet = ClinicalAssertionTraitSet(
          id = setId,
          clinicalAssertionTraitIds = traits.map(_.id),
          `type` = rawSet.tryExtract[String]("@Type"),
          content = Content.encode(rawSet)
        )
        (Some(traitSet), traits)
    }

  /**
    * TODO
    *
    * @param metadata TODO
    * @param referenceTraits TODO
    * @param mappings TODO
    */
  def findMatchingTrait(
    metadata: TraitMetadata,
    referenceTraits: Array[Trait],
    mappings: Array[TraitMapping]
  ): Option[Trait] =
    if (mappings.isEmpty) {
      // Lack of trait mappings means the VCV contains at most one trait,
      // which all the attached SCV traits should link to.
      referenceTraits.headOption
    } else {
      // Look through the reference traits to see if there are any with aligned medgen IDs.
      val medgenDirectMatch = referenceTraits.find(_.medgenId == metadata.medgenId)

      // Look to see if there are any with aligned XRefs.
      val xrefDirectMatch =
        referenceTraits.find(!_.xrefs.intersect(metadata.xrefs).isEmpty)

      // Find the reference trait with the matching MedGen ID if it's defined.
      // Otherwise match on preferred name.
      medgenDirectMatch
        .orElse(xrefDirectMatch)
        .orElse {
          // Look through the trait mappings for one that aligns with
          // the SCV's data.
          val matchingMapping = mappings.find { candidateMapping =>
            val sameTraitType = metadata.`type`.contains(candidateMapping.traitType)

            val nameMatch = {
              val isNameMapping = candidateMapping.mappingType == "Name"
              val isPreferredMatch = candidateMapping.mappingRef == "Preferred" &&
                metadata.name.contains(candidateMapping.mappingValue)
              val isAlternateMatch = candidateMapping.mappingRef == "Alternate" &&
                metadata.alternateNames.contains(candidateMapping.mappingValue)

              isNameMapping && (isPreferredMatch || isAlternateMatch)
            }

            val xrefMatch = {
              val isXrefMapping = candidateMapping.mappingType == "XRef"
              val xrefMatches = metadata.xrefs.exists { xref =>
                xref.db == candidateMapping.mappingRef &&
                xref.id == candidateMapping.mappingValue
              }

              isXrefMapping && xrefMatches
            }

            sameTraitType && (nameMatch || xrefMatch)
          }

          // Find the MedGen ID / name to look for in the VCV traits.
          val matchingMedgenId = matchingMapping.flatMap(_.medgenId)
          val matchingName = matchingMapping.flatMap(_.medgenName)

          // return the trait mapping medgen ID match or the name match
          referenceTraits
            .find(_.medgenId == matchingMedgenId)
            .orElse(referenceTraits.find(_.name == matchingName))
        }
    }

  /**
    * TODO
    *
    * @param assertionAccession TODO
    * @param rawAssertion TODO
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
