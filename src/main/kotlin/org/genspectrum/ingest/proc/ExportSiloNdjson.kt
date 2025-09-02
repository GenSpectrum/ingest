package org.genspectrum.ingest.proc

import org.genspectrum.ingest.entry.MutableEntry
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.silo.SiloEntry
import org.genspectrum.ingest.silo.SiloEntryAlignedSequence
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.readNdjson
import org.genspectrum.ingest.util.writeFile
import org.genspectrum.ingest.util.writeNdjson
import java.nio.file.Path

fun exportSiloNdjson(inputMutableEntryFile: File, outputSiloNdjsonPath: Path): File {
    require(inputMutableEntryFile.type == FileType.NDJSON)
    val outputFile = File(
        inputMutableEntryFile.name,
        outputSiloNdjsonPath,
        inputMutableEntryFile.sorted,
        FileType.NDJSON,
        Compression.ZSTD
    )

    val reader = readNdjson<MutableEntry>(readFile(inputMutableEntryFile.path))
    val writer = writeNdjson<SiloEntry>(writeFile(outputFile.path))
    for (entry in reader) {
        val siloEntry = entry.metadata +
                transformUnalignedSequences(entry.unalignedNucleotideSequences) +
                transformAlignedSequences(entry.alignedNucleotideSequences, entry.nucleotideInsertions) +
                transformAlignedSequences(entry.alignedAminoAcidSequences, entry.aminoAcidInsertions)
        writer.write(siloEntry)
    }
    writer.close()

    return outputFile
}

private fun transformUnalignedSequences(sequences: Map<String, String?>): Map<String, String?> {
    return sequences.map { (segment, sequence) -> "unaligned_$segment" to sequence }.toMap()
}

private fun transformAlignedSequences(
    sequences: Map<String, String?>,
    insertions: Map<String, List<String>>
): Map<String, SiloEntryAlignedSequence> {
    val transformed = mutableMapOf<String, SiloEntryAlignedSequence>()
    for ((segment, sequence) in sequences) {
        if (sequence != null) {
            transformed[segment] = SiloEntryAlignedSequence(sequence, insertions[segment] ?: emptyList())
        }
    }
    return transformed
}
