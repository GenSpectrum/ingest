package org.genspectrum.ingest

data class MutableEntry(
    var metadata: MutableMap<String, Any?>,
    var unalignedNucleotideSequences: MutableMap<String, String?>,
    var alignedNucleotideSequences: MutableMap<String, String?>,
    var alignedAminoAcidSequences: MutableMap<String, String?>,
    var nucleotideInsertions: MutableMap<String, MutableList<String>>,
    var aminoAcidInsertions: MutableMap<String, MutableList<String>>
)
