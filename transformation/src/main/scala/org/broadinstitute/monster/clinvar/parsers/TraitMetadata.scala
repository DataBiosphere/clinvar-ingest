package org.broadinstitute.monster.clinvar.parsers

import org.broadinstitute.monster.clinvar.Constants
import org.broadinstitute.monster.clinvar.jadeschema.struct.Xref
import upack.Msg

/**
  * Representation of metadata common to both types of ClinVar traits.
  *
  * @param id identifier for the trait
  * @param `type` type of the trait
  * @param name preferred name of the trait
  * @param alternateNames alternative names for the trait
  * @param medgenId ID of the trait in NCBI's MedGen database
  * @param xrefs links to the trait in other databases
  */
case class TraitMetadata(
  id: String,
  `type`: Option[String],
  name: Option[String],
  alternateNames: Array[String],
  medgenId: Option[String],
  xrefs: Array[Xref]
)

object TraitMetadata {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Extract common metadata from an unmodeled trait payload.
    *
    * @param rawTrait a raw trait payload, nested under either an Interpretation
    *                 or an SCV
    * @param getId function which can get a unique ID for the specific
    *              type of trait
    */
  def fromRawTrait(rawTrait: Msg)(getId: Msg => String): TraitMetadata = {
    // Extract the ID from the trait so we can use it in any raised errors.
    val id = getId(rawTrait)

    // Process any names stored in the trait.
    // Any nested xrefs will be "unzipped" from their names, to be
    // combined with top-level refs in the next step.
    val allNames = rawTrait.tryExtract[Array[Msg]]("Name").getOrElse(Array.empty)
    val (preferredName, alternateNames, nameXrefs) =
      allNames.foldLeft((Option.empty[String], List.empty[String], Set.empty[Xref])) {
        case ((prefAcc, altAcc, xrefAcc), name) =>
          val nameValue = name.extract[Msg]("ElementValue")
          val nameType = nameValue.extract[String]("@Type")
          val nameString = nameValue.extract[String]("$")
          val nameRefs = extractXrefs(name, Some(nameString)).toSet

          if (nameType == "Preferred") {
            if (prefAcc.isDefined) {
              throw new IllegalStateException(s"Trait $id has multiple preferred names")
            } else {
              (Some(nameString), altAcc, nameRefs.union(xrefAcc))
            }
          } else {
            (prefAcc, nameString :: altAcc, nameRefs.union(xrefAcc))
          }
      }
    // Process remaining xrefs in the payload, combining them with the
    // name-based refs.
    val topLevelRefs = extractXrefs(rawTrait, None)
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
      medgenId = medgenId,
      `type` = rawTrait.tryExtract[String]("@Type"),
      name = preferredName,
      alternateNames = alternateNames.toArray.sorted,
      xrefs = finalXrefs.toArray.sortBy(xref => (xref.db, xref.id, xref.sourceName))
    )
  }

  /**
    * Pull raw xrefs out of a containing payload, and process them.
    *
    * @param xrefContainer raw payload which might contain xrefs under the "XRef" key
    * @param linkedName name that should be associated with the extracted xrefs, if any
    */
  def extractXrefs(xrefContainer: Msg, linkedName: Option[String]): Array[Xref] =
    xrefContainer
      .tryExtract[Array[Msg]]("XRef")
      .getOrElse(Array.empty)
      .map { rawXref =>
        Xref(
          rawXref.extract[String]("@DB"),
          rawXref.extract[String]("@ID"),
          linkedName
        )
      }
}
