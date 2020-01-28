package org.broadinstitute.monster.clinvar.parsed

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger

import org.broadinstitute.monster.clinvar.jadeschema.struct.InterpretationComment
import org.broadinstitute.monster.clinvar.{Constants, Content}
import org.broadinstitute.monster.clinvar.jadeschema.table._
import upack.{Arr, Msg}

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
    val submitter = Submitter(
      id = rawAssertion.extract[String]("ClinVarAccession", "@OrgID"),
      submitterName =
        rawAssertion.tryExtract[String]("ClinVarAccession", "@SubmitterName"),
      orgCategory =
        rawAssertion.tryExtract[String]("ClinVarAccession", "@OrganizationCategory"),
      orgAbbrev = rawAssertion.tryExtract[String]("ClinVarAccession", "@OrgAbbreviation")
    )
    val submission = {
      val date = rawAssertion.extract[LocalDate]("@SubmissionDate")
      val additionalSubmitters = rawAssertion
        .tryExtract[Array[Msg]]("AdditionalSubmitters", "SubmitterDescription")
        .getOrElse(Array.empty)

      Submission(
        id = s"${submitter.id}.$date",
        submitterId = submitter.id,
        additionalSubmitterIds = additionalSubmitters.map(_.extract[String]("@OrgID")),
        submissionDate = date,
        submissionNames = rawAssertion
          .tryExtract[Array[Msg]]("SubmissionNameList", "SubmissionName")
          .getOrElse(Array.empty)
          .map(_.extract[String]("$"))
      )
    }

    // Extract the top-level set of traits described by the assertion.
    val assertionId = rawAssertion.extract[String]("@ID")
    val assertionAccession =
      rawAssertion.extract[String]("ClinVarAccession", "@Accession")
    val relevantTraitMappings = mappingsById.getOrElse(assertionId, Array.empty)

    val (directTraitSet, directTraits) =
      rawAssertion.tryExtract[Msg]("TraitSet") match {
        case Some(rawSet) =>
          val (set, traits) = parseRawTraitSet(
            assertionAccession,
            rawSet,
            interpretation.traits,
            relevantTraitMappings
          )
          (Some(set), traits)
        case None => (None, Array.empty[ClinicalAssertionTrait])
      }

    // Extract info about any other traits observed in the submission.
    val (traitSets, traits, observations) = {
      val observationCounter = new AtomicInteger(0)
      val zero = (
        directTraitSet.toArray,
        directTraits,
        Array.empty[ClinicalAssertionObservation]
      )
      rawAssertion
        .tryExtract[Array[Msg]]("ObservedInList", "ObservedIn")
        .getOrElse(Array.empty)
        .foldLeft(zero) {
          case ((traitSetAcc, traitAcc, observationAcc), rawObservation) =>
            val observationId =
              s"$assertionAccession.${observationCounter.getAndIncrement()}"
            val (observedTraitSet, observedTraits) =
              rawObservation.tryExtract[Msg]("TraitSet") match {
                case Some(rawSet) =>
                  val (set, traits) = parseRawTraitSet(
                    observationId,
                    rawSet,
                    interpretation.traits,
                    relevantTraitMappings
                  )
                  (Some(set), traits)
                case None => (None, Array.empty[ClinicalAssertionTrait])
              }

            val observation = ClinicalAssertionObservation(
              id = observationId,
              clinicalAssertionTraitSetId = observedTraitSet.map(_.id),
              content = Content.encode(rawObservation)
            )

            (
              observedTraitSet.toArray ++ traitSetAcc,
              observedTraits ++ traitAcc,
              observation +: observationAcc
            )
        }
    }

    // Extract the tree of variation records stored in the submission.
    val variations = {
      val buffer = new mutable.ArrayBuffer[ClinicalAssertionVariation]()
      val counter = new AtomicInteger(0)

      // Process the tree of variations, if present.
      // Since SCV variations can't be meaningfully cross-linked between archives,
      // we have to extract the entire payload of every variant in the tree here,
      // instead of just their IDs.
      val _ = extractVariationTree(rawAssertion, buffer) { (rawVariation, subtype) =>
        ClinicalAssertionVariation(
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
      }

      buffer.toArray
    }

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
      traitSets = traitSets,
      traits = traits
    )
  }

  /**
    * Convert a raw TraitSet paylod into our model.
    *
    * @param setId ID to assign to the parsed set
    * @param rawTraitSet payload to parse
    * @param referenceTraits curated traits from the same archive as the payload
    * @param traitMappings mappings from raw traits to curated traits
    */
  def parseRawTraitSet(
    setId: String,
    rawTraitSet: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): (ClinicalAssertionTraitSet, Array[ClinicalAssertionTrait]) = {
    val counter = new AtomicInteger(0)
    val traits = rawTraitSet
      .extract[Array[Msg]]("Trait")
      .map { rawTrait =>
        parseRawTrait(
          s"$setId.${counter.getAndIncrement()}",
          rawTrait,
          referenceTraits,
          traitMappings
        )
      }
    val traitSet = ClinicalAssertionTraitSet(
      id = setId,
      clinicalAssertionTraitIds = traits.map(_.id),
      `type` = rawTraitSet.tryExtract[String]("@Type"),
      content = Content.encode(rawTraitSet)
    )

    (traitSet, traits)
  }

  /**
    * Convert a raw Trait payload into our model.
    *
    * @param traitId ID to assign to the parsed trait
    * @param rawTrait payload to parse
    * @param referenceTraits curated traits from the same archive as the payload
    * @param traitMappings mappings from raw traits to curated traits
    */
  def parseRawTrait(
    traitId: String,
    rawTrait: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): ClinicalAssertionTrait = {
    // Extract common metadata from the trait.
    val metadata = TraitMetadata.fromRawTrait(rawTrait)(_ => traitId)

    val matchingTrait = if (traitMappings.isEmpty) {
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
          val matchingMapping = traitMappings.find { candidateMapping =>
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

  /**
    * Traverse the tree of SCV variations, parsing each one and adding it to a buffer.
    *
    * @param variationWrapper raw variation payload
    * @param buffer buffer where parsed variations should be stored
    * @param parse function which will convert a raw variation payload + subclass type
    *              into an SCV variation model
    *
    * @return a tuple where the first element contains the IDs of the variation's
    *         immediate children, and the second contains the IDs of all other
    *         descendants of the variation
    */
  def extractVariationTree(
    variationWrapper: Msg,
    buffer: mutable.ArrayBuffer[ClinicalAssertionVariation]
  )(parse: (Msg, String) => ClinicalAssertionVariation): (List[String], List[String]) = {
    val zero = (List.empty[String], List.empty[String])

    // Common logic for processing a single child of the variationWrapper.
    def processChild(rawChild: Msg, subtype: Msg): (String, List[String]) = {
      val baseVariation = parse(rawChild, subtype.str)
      val (grandchildIds, deepIds) = extractVariationTree(rawChild, buffer)(parse)
      buffer.append {
        baseVariation.copy(
          childIds = grandchildIds.toArray,
          descendantIds = (grandchildIds ::: deepIds).toArray,
          content = Content.encode(rawChild)
        )
      }
      (baseVariation.id, grandchildIds ::: deepIds)
    }

    Constants.VariationTypes.foldLeft(zero) {
      case ((childAcc, descendantsAcc), subtype) =>
        val (childIds, descendantIds) = variationWrapper.obj.remove(subtype).fold(zero) {
          case Arr(children) =>
            children.foldLeft(zero) {
              case ((childAcc, descendantsAcc), child) =>
                val (childId, descendantIds) = processChild(child, subtype)
                (childId :: childAcc, descendantIds ::: descendantsAcc)
            }
          case child =>
            val (childId, descendantIds) = processChild(child, subtype)
            (List(childId), descendantIds)
        }
        (childIds ::: childAcc, descendantIds ::: descendantsAcc)
    }
  }
}
