package org.genspectrum.ingest

import org.genspectrum.ingest.utils.SortedNdjsonFilesOuterJoiner
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class JoinSC2NextstrainOpenData {

    fun run(
        sortedMetadataFile: Path,
        sortedSequencesFile: Path,
        sortedAlignedFile: Path,
        sortedTranslationFiles: List<Pair<String, Path>>,
        outputPath: Path
    ) {
        val translationNames = sortedTranslationFiles.map { it.first }
        val translationPaths = sortedTranslationFiles.map { it.second }
        val paths = mutableListOf(sortedMetadataFile, sortedSequencesFile, sortedAlignedFile)
        paths.addAll(translationPaths)
        val inputStreams = paths.map { readFile(it) }

        val joiner = SortedNdjsonFilesOuterJoiner("strain", inputStreams)
        val writer = writeNdjson<Any>(writeFile(outputPath))
        for ((_, values) in joiner) {
            val metadataEntry = values[0] ?: continue
            val sequenceEntry = values[1]
            val alignedEntry = values[2]
            val translationSequences = values.subList(3, values.size).withIndex()
                .map { (index, entry) -> translationNames[index] to entry?.getString("sequence") }
            val joined = mapOf(
                "metadata" to metadataEntry,
                "unalignedNucleotideSequences" to mapOf("main" to sequenceEntry?.getString("sequence")),
                "alignedNucleotideSequences" to mapOf("main" to alignedEntry?.getString("sequence")),
                "alignedAminoAcidSequences" to translationSequences.toMap()
            )
            writer.write(joined)
        }
        writer.close()
    }
}
