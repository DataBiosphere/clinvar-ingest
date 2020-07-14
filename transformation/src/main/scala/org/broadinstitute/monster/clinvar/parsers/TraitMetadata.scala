package org.broadinstitute.monster.clinvar.parsers

import java.time.LocalDate

import org.broadinstitute.monster.clinvar.Constants
import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
import upack.Msg

/**
  * Representation of metadata common to both types of ClinVar traits.
  *
  * @param id identifier for the trait
  * @param releaseDate datestamp of the ClinVar release containing this trait record
  * @param `type` type of the trait
  * @param name preferred name of the trait
  * @param alternateNames alternative names for the trait
  * @param medgenId ID of the trait in NCBI's MedGen database
  * @param xrefs links to the trait in other databases
  */
case class TraitMetadata(
  id: String,
  releaseDate: LocalDate,
  `type`: Option[String],
  name: Option[String],
  alternateNames: List[String],
  medgenId: Option[String],
  xrefs: Set[Xref]
)

object TraitMetadata {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Interface for a utility which can extract common metadata
    * from unmodeled Trait payloads.
    * */
  trait Parser extends Serializable {

    /**
      * Extract common metadata from an unmodeled Trait payload.
      *
      * @param id unique ID to assign to the trait
      * @param rawTrait a raw trait payload, nested under either an Interpretation
      *                 or an SCV
      */
    def parse(id: String, rawTrait: Msg): TraitMetadata
  }

  /** Parser for "real" Trait payloads, to be used in production. */
  def parser(releaseDate: LocalDate): Parser = (id, rawTrait) => {
    // Process any names stored in the trait.
    // Any nested xrefs will be "unzipped" from their names, to be
    // combined with top-level refs in the next step.
    val allNames = rawTrait.tryExtract[List[Msg]]("Name").getOrElse(List.empty)
    val (preferredName, alternateNames, nameXrefs) =
      allNames.foldLeft((Option.empty[String], List.empty[String], Set.empty[Xref])) {
        case ((prefAcc, altAcc, xrefAcc), name) =>
          val nameValue = name.extract[Msg]("ElementValue")
          val nameType = nameValue.extract[String]("@Type")
          val nameString = nameValue.extract[String]("$")

          if (nameType == "Preferred") {
            if (prefAcc.isDefined) {
              throw new IllegalStateException(s"Trait $id has multiple preferred names")
            } else {
              val preferredRefs = extractXrefs(name, Some("name"), None).toSet
              (Some(nameString), altAcc, preferredRefs.union(xrefAcc))
            }
          } else {
            val alternateRefs = extractXrefs(name, Some("alternate_names"), Some(nameString)).toSet
            (prefAcc, nameString :: altAcc, alternateRefs.union(xrefAcc))
          }
      }
    // Process remaining xrefs in the payload, combining them with the
    // name-based refs.
    val topLevelRefs = extractXrefs(rawTrait, None, None)
    val (medgenId, finalXrefs) =
      topLevelRefs.foldLeft((Option.empty[String], nameXrefs)) {
        case ((medgenAcc, xrefAcc), xref) =>
          if (xref.db.contains(Constants.MedGenKey)) {
            if (medgenAcc.isDefined) {
              throw new IllegalStateException(s"Trait $id contains two MedGen references")
            } else {
              (Some(xref.id), xrefAcc)
            }
          } else {
            (medgenAcc, xrefAcc + xref)
          }
      }

    TraitMetadata(
      id = id,
      releaseDate = releaseDate,
      medgenId = medgenId,
      `type` = rawTrait.tryExtract[String]("@Type"),
      name = preferredName,
      alternateNames = alternateNames.sorted,
      xrefs = finalXrefs
    )
  }

  /**
    * Pull raw xrefs out of a containing payload, and process them.
    *
    * @param xrefContainer raw payload which might contain xrefs under the "XRef" key
    * @param referencedField name of the field in the output JSON associated with the XRef
    * @param referencedElement specific element in the referenced field associated with the XRef,
    *                          only needed for List fields
    */
  def extractXrefs(
    xrefContainer: Msg,
    referencedField: Option[String],
    referencedElement: Option[String]
  ): List[Xref] =
    xrefContainer
      .tryExtract[List[Msg]]("XRef")
      .getOrElse(List.empty)
      .map { rawXref =>
        Xref(
          db = rawXref.extract[String]("@DB"),
          id = rawXref.extract[String]("@ID"),
          `type` = rawXref.tryExtract[String]("@Type"),
          refField = referencedField,
          refFieldElement = referencedElement
        )
      }
}
