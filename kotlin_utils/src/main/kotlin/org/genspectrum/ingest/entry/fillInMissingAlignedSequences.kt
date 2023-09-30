package org.genspectrum.ingest.entry

import org.genspectrum.ingest.AlignedGenome

/**
 * This function replaces all aligned sequences that are null with the value provided in "template". This is useful
 * to replace nulls with sequences filled with N or X.
 */
fun MutableEntry.fillInMissingAlignedSequences(template: AlignedGenome) {
    for (name in template.nucleotideSequences.keys) {
        if (this.alignedNucleotideSequences[name] == null) {
            this.alignedNucleotideSequences[name] = template.nucleotideSequences[name]
        }
    }
    for (name in template.aminoAcidSequences.keys) {
        if (this.alignedAminoAcidSequences[name] == null) {
            this.alignedAminoAcidSequences[name] = template.aminoAcidSequences[name]
        }
    }
}
