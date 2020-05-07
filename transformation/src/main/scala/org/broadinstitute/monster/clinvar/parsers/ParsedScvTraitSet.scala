package org.broadinstitute.monster.clinvar.parsers

import java.util.concurrent.atomic.AtomicInteger

import org.broadinstitute.monster.clinvar.Content
import org.broadinstitute.monster.clinvar.jadeschema.table.{
  ClinicalAssertionTrait,
  ClinicalAssertionTraitSet,
  Trait,
  TraitMapping
}
import upack.Msg

/**
  * Wrapper for fully-parsed contents of a condition-set supporting
  * a submission to ClinVar.
  *
  * @param traitSet info about the group of conditions itself
  * @param traits info about the individual conditions contained in the group
  */
case class ParsedScvTraitSet(
  traitSet: ClinicalAssertionTraitSet,
  traits: Array[ClinicalAssertionTrait]
)

object ParsedScvTraitSet {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Extract a condition-set from a raw payload, if one is present.
    *
    * @param setId ID which should be assigned to the set
    * @param rawWrapper raw payload which might contain a TraitSet node
    * @param referenceTraits reference traits which any extracted set should
    *                        be linked against
    * @param traitMappings mappings to use for linking the extracted set
    *                      to reference traits
    */
  def fromRawSetWrapper(
    setId: String,
    rawWrapper: Msg,
    referenceTraits: Array[Trait],
    traitMappings: Array[TraitMapping]
  ): Option[ParsedScvTraitSet] =
    rawWrapper.tryExtract[Msg]("TraitSet").map { rawSet =>
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
            xrefs = metadata.xrefs.toArray
              .sortBy(xref => (xref.db, xref.id, xref.`type`, xref.refField, xref.refFieldElement)),
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

      ParsedScvTraitSet(traitSet, traits)
    }

  /**
    * Search through reference traits to find one that links to a
    * submitted trait model.
    *
    * @param metadata information about a submitted trait
    * @param referenceTraits reference traits to search through
    * @param mappings mappings between submitted and reference traits
    */
  private def findMatchingTrait(
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
      // NOTE: The flatMap is _required_ here to prevent a false match on None == None.
      val medgenDirectMatch = metadata.medgenId.flatMap { knownMedgenId =>
        referenceTraits.find(_.medgenId.contains(knownMedgenId))
      }

      // Look to see if there are any with aligned XRefs.
      val xrefDirectMatch =
        referenceTraits.find(_.xrefs.toSet.intersect(metadata.xrefs).nonEmpty)

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
}
