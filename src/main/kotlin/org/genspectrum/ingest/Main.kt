package org.genspectrum.ingest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import org.genspectrum.ingest.workflows.SC2GisaidWorkflow
import org.genspectrum.ingest.workflows.SC2NextstrainOpenWorkflow
import java.time.LocalDateTime
import kotlin.io.path.Path
import kotlin.time.measureTime

class Ingest : CliktCommand() {
    override fun run() = Unit
}

class SC2NextstrainOpenIngestCommand : CliktCommand(name = "ingest-sc2-nextstrain-open") {
    private val workdirPath by argument("workdir")

    override fun run() {
        SC2NextstrainOpenWorkflow().run(Path(workdirPath))
    }
}

class SC2GisaidIngestCommand : CliktCommand(name = "ingest-sc2-gisaid") {
    private val workdirPath by argument("workdir")
    private val url by argument("url")
    private val user by argument("user")
    private val password by argument("password")

    override fun run() {
        SC2GisaidWorkflow().run(
            Path(workdirPath),
            url, user, password,
            Path(workdirPath).resolve("00_archive/TODO"),
            Path(workdirPath).resolve("00_archive/provision.00.hashes.ndjson.zst")
        )
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
                        SC2NextstrainOpenIngestCommand(),
                        SC2GisaidIngestCommand()
                    )
                    .main(args)
            }
            println("Elapsed: $elapsed")
        }
    }
}
