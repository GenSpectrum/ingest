package org.genspectrum.ingest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import kotlin.io.path.Path

class Ingest : CliktCommand() {
    override fun run() = Unit
}

class SortFasta : CliktCommand(name = "fasta-to-sorted-ndjson") {
    private val inputPath by argument("input_file")
    private val outputPath by argument("output_file")
    private val workdir by argument("workdir")

    override fun run() {
        FastaToSortedNdjson().run(Path(inputPath), Path(outputPath), Path(workdir))
    }
}

fun main(args: Array<String>) = Ingest()
    .subcommands(SortFasta())
    .main(args)
