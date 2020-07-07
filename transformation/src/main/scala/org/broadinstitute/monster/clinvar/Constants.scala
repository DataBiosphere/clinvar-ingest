package org.broadinstitute.monster.clinvar

/** Container for constants that we use across the ClinVar pipeline. */
object Constants {
  /** Supported types of variants in VCVs and SCVs. */
  val VariationTypes: List[String] = List("SimpleAllele", "Haplotype", "Genotype")

  /** Constant used to mark references to the MedGen database. */
  val MedGenKey: String = "MedGen"
}
