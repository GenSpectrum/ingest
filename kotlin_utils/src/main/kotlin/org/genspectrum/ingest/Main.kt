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

class TsvToNdjsonCommand : CliktCommand(name = "tsv-to-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        TsvToNdjson().run(Path(inputPath), Path(outputPath))
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

class JoinSC2NextstrainOpenDataCommand : CliktCommand(name = "join-sc2-nextstrain-open-data") {
    private val metadataPath by option("--sorted-metadata").required()
    private val sequencesPath by option("--sorted-sequences").required()
    private val alignedPath by option("--sorted-aligned").required()
    private val translationEPath by option("--sorted-translation-e").required()
    private val outputPath by option("--output").required()

    override fun run() {
        JoinSC2NextstrainOpenData().run(
            Path(metadataPath),
            Path(sequencesPath),
            Path(alignedPath),
            listOf(
                "E" to Path(translationEPath)
            ),
            Path(outputPath)
        )
    }
}

fun main(args: Array<String>) {
    println("Program started at ${LocalDateTime.now()}")
    val elapsed = measureTime {
        Ingest()
            .subcommands(
                FastaToNdjsonCommand(),
                TsvToNdjsonCommand(),
                SortNdjsonCommand(),
                JoinSC2NextstrainOpenDataCommand()
            )
            .main(args)
    }
    println("Elapsed: $elapsed")
}
