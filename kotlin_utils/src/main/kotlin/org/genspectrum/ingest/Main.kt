package org.genspectrum.ingest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import kotlin.io.path.Path
import kotlin.time.measureTime

class Ingest : CliktCommand() {
    override fun run() = Unit
}

class FastaToNdjsonCommand : CliktCommand(name = "fasta-to-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")

    override fun run() {
        FastaToNdjson().run(Path(inputPath), Path(outputPath))
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

fun main(args: Array<String>) {
    val elapsed = measureTime {
        Ingest()
            .subcommands(FastaToNdjsonCommand(), TsvToNdjsonCommand(), SortNdjsonCommand())
            .main(args)
    }
    println("Elapsed: $elapsed")
}
