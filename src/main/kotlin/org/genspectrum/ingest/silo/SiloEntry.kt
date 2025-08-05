package org.genspectrum.ingest.silo

typealias SiloEntry = Map<String, Any?>

data class SiloEntryAlignedSequence(
    val sequence: String,
    val insertions: List<String>,
)
