package org.genspectrum.ingest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import java.time.LocalDateTime
import kotlin.io.path.Path
import kotlin.time.measureTime

class Ingest : CliktCommand() {
    override fun run() = Unit
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

class SortNdjsonCommand : CliktCommand(name = "sort-ndjson") {
    private val sortBy by argument("sort_by")
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")
    private val workdirPath by argument("workdir")

    override fun run() {
        SortNdjson().run(sortBy, Path(inputPath), Path(outputPath), Path(workdirPath))
    }
}

class TsvToNdjsonCommand : CliktCommand(name = "tsv-to-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        TsvToNdjson().run(Path(inputPath), Path(outputPath))
    }
}

class Main {
    companion object {
        @JvmStatic
        fun main(args : Array<String>) {
            println("Program started at ${LocalDateTime.now()}")
            val elapsed = measureTime {
                Ingest()
                    .subcommands(
                        FastaToNdjsonCommand(),
                        JoinSC2NextstrainOpenDataCommand(),
                        NoopNdjsonCommand(),
                        SortNdjsonCommand(),
                        TsvToNdjsonCommand(),
                    )
                    .main(args)
            }
            println("Elapsed: $elapsed")
        }
    }
}
