package org.genspectrum.ingest

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import org.genspectrum.ingest.file.AllPangoLineagesFile
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.workflow.runSC2GisaidWorkflow
import org.genspectrum.ingest.workflow.runSC2NextstrainOpenWorkflow
import java.time.LocalDateTime
import kotlin.io.path.Path
import kotlin.time.measureTime

class Ingest : CliktCommand() {
    override fun run() = Unit
}

class SC2NextstrainOpenIngestCommand : CliktCommand(name = "ingest-sc2-nextstrain-open") {
    private val workdirPath by argument("workdir")

    override fun run() {
        runSC2NextstrainOpenWorkflow(Path(workdirPath))
    }
}

class SC2GisaidIngestCommand : CliktCommand(name = "ingest-sc2-gisaid") {
    private val workdirPath by argument("workdir")
    private val previousProcessedVersion by argument("previous-processed")
    private val url by argument("url")
    private val user by argument("user")
    private val password by argument("password")
    private val geoLocationRulesFile by argument("geo-location-rules")

    override fun run() {
        val previousProcessedVersionDir = Path(workdirPath)
            .resolve("00_archive")
            .resolve(previousProcessedVersion)
        runSC2GisaidWorkflow(
            workdir = Path(workdirPath),
            url = url,
            user = user,
            password = password,
            previousProcessed = File(
                "provision.$previousProcessedVersion",
                previousProcessedVersionDir,
                false,
                FileType.NDJSON,
                Compression.ZSTD
            ),
            previousAllPangoLineagesFile = AllPangoLineagesFile(
                previousProcessedVersion,
                previousProcessedVersionDir,
            ),
            previousHashes = previousProcessedVersionDir.resolve("provision.$previousProcessedVersion.hashes.ndjson.zst"),
            geoLocationRulesFile = Path(geoLocationRulesFile)
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
