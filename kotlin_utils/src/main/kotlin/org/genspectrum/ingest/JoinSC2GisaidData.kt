package org.genspectrum.ingest

import com.alibaba.fastjson2.to
import org.genspectrum.ingest.utils.SortedNdjsonFilesOuterJoiner
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class JoinSC2GisaidData {

    fun run(
        sortedProvisionFile: Path,
        sortedNextcladeFile: Path,
        sortedAlignedFile: Path,
        sortedTranslationFiles: List<Pair<String, Path>>,
        outputPath: Path
    ) {
        val translationNames = sortedTranslationFiles.map { it.first }
        val translationPaths = sortedTranslationFiles.map { it.second }
        val paths = mutableListOf(sortedProvisionFile, sortedNextcladeFile, sortedAlignedFile)
        paths.addAll(translationPaths)
        val inputStreams = paths.map { readFile(it) }

        val joiner = SortedNdjsonFilesOuterJoiner("id", "seqName", inputStreams)
        val writer = writeNdjson<Any>(writeFile(outputPath))
        for ((_, values) in joiner) {
            val provisionEntry = values[0]?.to<MutableEntry>() ?: continue
            val nextcladeEntry = values[1]?.toMap()
            if (nextcladeEntry != null) {
                provisionEntry.metadata += nextcladeEntry
            }



            val metadataEntry = (values[0]?.toMap() ?: continue).toMutableMap()

            val sequenceEntry = values[2]
            val alignedEntry = values[3]
            val translationSequences = values.subList(4, values.size).withIndex()
                .map { (index, entry) -> translationNames[index] to entry?.getString("sequence") }
            val nucleotideInsertionsText = nextcladeEntry?.get("insertions") as String?
            val nucleotideInsertions = mutableListOf<String>()
            if (!nucleotideInsertionsText.isNullOrBlank()) {
                nucleotideInsertions.addAll(nucleotideInsertionsText.split(","))
            }
            val aminoAcidInsertionsText = nextcladeEntry?.get("aaInsertions") as String?
            val aminoAcidInsertionsLists = sortedTranslationFiles.map { it.first to mutableListOf<String>() }
                .toTypedArray()
            val aminoAcidInsertions = mutableMapOf(*aminoAcidInsertionsLists)
            if (!aminoAcidInsertionsText.isNullOrBlank()) {
                aminoAcidInsertionsText
                    .split(",")
                    .forEach {
                        val (gene, insertion) = it.split(":", limit = 2)
                        aminoAcidInsertions[gene]!!.add(insertion)
                    }
            }
            val joined = MutableEntry(
                metadataEntry["strain"] as String,
                metadataEntry,
                mutableMapOf("main" to sequenceEntry?.getString("sequence")),
                mutableMapOf("main" to alignedEntry?.getString("sequence")),
                translationSequences.toMap().toMutableMap(),
                mutableMapOf("main" to nucleotideInsertions),
                aminoAcidInsertions
            )
            writer.write(joined)
        }
        writer.close()
    }
}
