package org.genspectrum.ingest

import com.alibaba.fastjson2.JSONObject
import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.genspectrum.ingest.proc.*
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.time.LocalDateTime
import kotlin.io.path.Path
import kotlin.time.measureTime

class Ingest : CliktCommand() {
    override fun run() = Unit
}

class CompareHashesCommand : CliktCommand(name = "compare-hashes") {
    private val idColumn by argument("id_column")
    private val oldHashesPath by argument("old_hashes")
    private val newHashesPath by argument("new_hashes")
    private val outputPath by argument("output_file")

    override fun run() {
        val results = CompareHashes().run(
            getHashEntries(oldHashesPath),
            getHashEntries(newHashesPath)
        )
        println("Added: ${results.added.size}, changed: ${results.changed.size}, " +
                "deleted: ${results.deleted.size}, unchanged: ${results.unchanged.size}, ")
        writeFile(Path(outputPath)).use { outputStream ->
            outputStream.write(results.toJSONByteArray(JSONWriter.Feature.LargeObject))
            outputStream.write("\n".toByteArray())
        }
    }

    private fun getHashEntries(path: String): List<HashEntry> {
        val reader = readNdjson<JSONObject>(readFile(Path(path)))
        return reader.map { HashEntry(it.getString(idColumn), it.getString("md5")) }.toList()
    }
}

class ExtractAddedOrChangedCommand : CliktCommand(name = "extract-added-or-changed") {
    private val idColumn by argument("id_column")
    private val changeComparisonPath by argument("change_comparison_file")
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        ExtractAddedOrChanged().run(idColumn, Path(changeComparisonPath), Path(inputPath), Path(outputPath))
    }
}

class FastaToNdjsonCommand : CliktCommand(name = "fasta-to-ndjson") {
    private val idColumn by argument("id_column")
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        FastaToNdjson().run(idColumn, Path(inputPath), Path(outputPath))
    }
}

class JoinSC2NextstrainOpenDataCommand : CliktCommand(name = "join-sc2-nextstrain-open-data") {
    private val metadataPath by option("--sorted-metadata").required()
    private val nextcladePath by option("--sorted-nextclade").required()
    private val sequencesPath by option("--sorted-sequences").required()
    private val alignedPath by option("--sorted-aligned").required()
    private val translationEPath by option("--sorted-translation-e").required()
    private val translationMPath by option("--sorted-translation-m").required()
    private val translationNPath by option("--sorted-translation-n").required()
    private val translationORF1aPath by option("--sorted-translation-orf1a").required()
    private val translationORF1bPath by option("--sorted-translation-orf1b").required()
    private val translationORF3aPath by option("--sorted-translation-orf3a").required()
    private val translationORF6Path by option("--sorted-translation-orf6").required()
    private val translationORF7aPath by option("--sorted-translation-orf7a").required()
    private val translationORF7bPath by option("--sorted-translation-orf7b").required()
    private val translationORF8Path by option("--sorted-translation-orf8").required()
    private val translationORF9bPath by option("--sorted-translation-orf9b").required()
    private val translationSPath by option("--sorted-translation-s").required()
    private val outputPath by option("--output").required()

    override fun run() {
        JoinSC2NextstrainOpenData().run(
            Path(metadataPath),
            Path(nextcladePath),
            Path(sequencesPath),
            Path(alignedPath),
            listOf(
                "E" to Path(translationEPath),
                "M" to Path(translationMPath),
                "N" to Path(translationNPath),
                "ORF1a" to Path(translationORF1aPath),
                "ORF1b" to Path(translationORF1bPath),
                "ORF3a" to Path(translationORF3aPath),
                "ORF6" to Path(translationORF6Path),
                "ORF7a" to Path(translationORF7aPath),
                "ORF7b" to Path(translationORF7bPath),
                "ORF8" to Path(translationORF8Path),
                "ORF9b" to Path(translationORF9bPath),
                "S" to Path(translationSPath)
            ),
            Path(outputPath)
        )
    }
}

class NoopNdjsonCommand : CliktCommand(name = "noop-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        NoopNdjson().run(Path(inputPath), Path(outputPath))
    }
}

/**
 * If multiple processing steps are chosen, the execution order is:
 *
 * 1. --rename-metadata
 * 2. --select-metadata
 * 3. --map-to-null
 * 4. --parse-date
 * 5. --parse-integer
 * 6. --parse-float
 * 7. --fill-in-missing-aligned=sequences
 */
class ProcessCommand : CliktCommand(name = "process-ndjson") {
    private val fillInMissingAlignedSequencesReferenceGenomePath by option("--fill-in-missing-aligned-sequences")
    private val mapToNull by option("--map-to-null").flag()
    private val parseDate by option("--parse-date").multiple()
    private val parseInteger by option("--parse-integer").multiple()
    private val parseFloat by option("--parse-float").multiple()
    private val renameMetadata by option("--rename-metadata").multiple()
    private val selectMetadata by option("--select-metadata")

    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        var oldToNewNames: List<Pair<String, String>>? = null
        if (renameMetadata.isNotEmpty()) {
            oldToNewNames = renameMetadata.map {
                val split = it.split(":")
                split[0] to split[1]
            }
        }
        val selectFields = selectMetadata?.split(",")?.toSet()
        var fillInMissingAlignedSequencesTemplate: AlignedGenome? = null
        if (fillInMissingAlignedSequencesReferenceGenomePath != null) {
            fillInMissingAlignedSequencesTemplate =
                AlignedGenome.loadFromFile(Path(fillInMissingAlignedSequencesReferenceGenomePath!!))
                    .replaceContentWithUnknown()
        }

        val reader = readNdjson<MutableEntry>(readFile(Path(inputPath)))
        val writer = writeNdjson<MutableEntry>(writeFile(Path(outputPath)))
        for (entry in reader) {
            if (oldToNewNames != null) {
                entry.renameMetadata(oldToNewNames)
            }
            if (selectFields != null) {
                entry.selectMetadata(selectFields)
            }
            if (mapToNull) {
                entry.mapToNull()
            }
            parseDate.forEach { entry.parseDate(it) }
            parseInteger.forEach { entry.parseInteger(it) }
            parseFloat.forEach { entry.parseFloat(it) }
            if (fillInMissingAlignedSequencesTemplate != null) {
                entry.fillInMissingAlignedSequences(fillInMissingAlignedSequencesTemplate)
            }
            writer.write(entry)
        }
        writer.close()
    }
}

class SortNdjsonCommand : CliktCommand(name = "sort-ndjson") {
    private val sortBy by argument("sort_by")
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")
    private val workdirPath by argument("workdir")

    override fun run() {
        SortNdjson().run(sortBy, Path(inputPath), Path(outputPath), Path(workdirPath))
    }
}

class TransformSC2GisaidBasicsCommand : CliktCommand(name = "transform-sc2-gisaid-basics") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")
    private val hashOutputPath by argument("hash_output_file")

    override fun run() {
        TransformSC2GisaidBasics().run(Path(inputPath), Path(outputPath), Path(hashOutputPath))
    }
}

class TsvToNdjsonCommand : CliktCommand(name = "tsv-to-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        TsvToNdjson().run(Path(inputPath), Path(outputPath))
    }
}

class UnalignedNucleotideSequencesToFastaCommand : CliktCommand(name = "unaligned-nucleotide-sequences-to-fasta") {
    private val idColumn by argument("id_column")
    private val sequenceName by argument("sequence_name")
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        UnalignedNucleotideSequencesToFasta().run(idColumn, sequenceName, Path(inputPath), Path(outputPath))
    }
}

class Main {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            println("Program started at ${LocalDateTime.now()}")
            val elapsed = measureTime {
                Ingest()
                    .subcommands(
                        CompareHashesCommand(),
                        ExtractAddedOrChangedCommand(),
                        FastaToNdjsonCommand(),
                        JoinSC2NextstrainOpenDataCommand(),
                        NoopNdjsonCommand(),
                        ProcessCommand(),
                        SortNdjsonCommand(),
                        TransformSC2GisaidBasicsCommand(),
                        TsvToNdjsonCommand(),
                        UnalignedNucleotideSequencesToFastaCommand(),
                    )
                    .main(args)
            }
            println("Elapsed: $elapsed")
        }
    }
}
