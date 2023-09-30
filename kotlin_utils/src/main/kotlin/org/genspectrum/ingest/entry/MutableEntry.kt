package org.genspectrum.ingest.entry

data class MutableEntry(
    var id: String,
    var metadata: MutableMap<String, Any?>,
    var unalignedNucleotideSequences: MutableMap<String, String?>,
    var alignedNucleotideSequences: MutableMap<String, String?>,
    var alignedAminoAcidSequences: MutableMap<String, String?>,
    var nucleotideInsertions: MutableMap<String, MutableList<String>>,
    var aminoAcidInsertions: MutableMap<String, MutableList<String>>
)
