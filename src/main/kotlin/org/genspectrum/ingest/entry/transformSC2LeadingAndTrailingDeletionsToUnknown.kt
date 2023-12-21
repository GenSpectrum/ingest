package org.genspectrum.ingest.entry


/**
 * This function transforms leading and trailing deletions in the aligned nucleotide sequence to N. It also
 * transforms leading deletions in the aligned amino acid sequence of ORF1a to X. Both are not great/accurate
 * but are consistent with LAPIS 1 [1, 2] and acceptable for now.
 *
 * [1] https://github.com/GenSpectrum/LAPIS/blob/fa70ac6b28102ed0b788859f4072d061aee54ab9/server/src/main/java/ch/ethz/lapis/source/MutationFinder.java#L14
 * [2] https://github.com/GenSpectrum/LAPIS/blob/fa70ac6b28102ed0b788859f4072d061aee54ab9/server/src/main/java/ch/ethz/lapis/source/gisaid/BatchProcessingWorker.java#L136
 * and
 */
fun MutableEntry.maskSC2LeadingAndTrailingDeletions() {
    val nucleotideSequence = this.alignedNucleotideSequences["main"]
    if (nucleotideSequence != null) {
        this.alignedNucleotideSequences["main"] = replaceTrailingDeletions(
            replaceLeadingDeletions(nucleotideSequence, 'N'), 'N')
    }
    val aminoAcidSequence = this.alignedAminoAcidSequences["ORF1a"]
    if (aminoAcidSequence != null) {
        this.alignedAminoAcidSequences["ORF1a"] = replaceLeadingDeletions(aminoAcidSequence, 'X')
    }
}

private fun replaceLeadingDeletions(seq: String, replaceWith: Char): String {
    val arr = seq.toCharArray()
    for ((i, c) in arr.withIndex()) {
        if (c != '-') {
            break
        }
        arr[i] = replaceWith
    }
    return String(arr)
}

private fun replaceTrailingDeletions(seq: String, replaceWith: Char): String {
    val arr = seq.toCharArray()
    for ((i, c) in arr.withIndex().reversed()) {
        if (c != '-') {
            break
        }
        arr[i] = replaceWith
    }
    return String(arr)
}
