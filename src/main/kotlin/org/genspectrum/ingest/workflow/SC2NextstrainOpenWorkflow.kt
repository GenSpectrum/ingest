package org.genspectrum.ingest.workflow

import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.proc.fastaToNdjson
import org.genspectrum.ingest.proc.joinSC2NextstrainOpenData
import org.genspectrum.ingest.proc.renameFile
import org.genspectrum.ingest.proc.sortNdjson
import org.genspectrum.ingest.proc.tsvToNdjson
import org.genspectrum.ingest.util.runParallel
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

fun runSC2NextstrainOpenWorkflow(workdir: Path) {
    val fromSourcePath = workdir.resolve("01_from_source")
    Files.createDirectories(fromSourcePath)
    val sourceFiles = runParallel(OpenFiles.entries.map { { it to downloadFromNextstrain(it, fromSourcePath) } }, 3).toMap()

    println("${LocalDateTime.now()}: Finished downloading from Nextstrain")

    val ndjsonPath = workdir.resolve("02_ndjson")
    Files.createDirectories(ndjsonPath)
    val ndjsonFiles = transformSourceFilesToNdjson(sourceFiles, ndjsonPath)

    println("${LocalDateTime.now()}: Finished transformSourceFilesToNdjson")

    val sortedPath = workdir.resolve("03_sorted")
    Files.createDirectories(sortedPath)
    val sortedFiles = sortNdjsonFiles(ndjsonFiles, sortedPath)

    println("${LocalDateTime.now()}: Finished sortNdjsonFiles")

    val joinedPath = workdir.resolve("04_joined_and_cleaned")
    Files.createDirectories(joinedPath)
    val joinedFile = joinFiles(sortedFiles, joinedPath)

    println("${LocalDateTime.now()}: Finished joinFiles")

    val finalDestinationPath = workdir.resolve("00_archive")
    Files.createDirectories(finalDestinationPath)
    val finalProvisionFile = moveFinalFile(
        joinedFile,
        finalDestinationPath
    )

    println("Final output: ${finalProvisionFile.path}")
}

private enum class OpenFiles {
    METADATA,
    NEXTCLADE,
    SEQUENCES,
    ALIGNED,
    TRANSLATION_E,
    TRANSLATION_M,
    TRANSLATION_N,
    TRANSLATION_ORF1a,
    TRANSLATION_ORF1b,
    TRANSLATION_ORF3a,
    TRANSLATION_ORF6,
    TRANSLATION_ORF7a,
    TRANSLATION_ORF7b,
    TRANSLATION_ORF8,
    TRANSLATION_ORF9b,
    TRANSLATION_S
}

private fun downloadFromNextstrain(file: OpenFiles, outputDirectory: Path): File {
    val tsvTemplate = File("-", outputDirectory, false, FileType.TSV, Compression.ZSTD)
    val fastaTemplate = File("-", outputDirectory, false, FileType.FASTA, Compression.ZSTD)
    val outputFile = when(file) {
        OpenFiles.METADATA -> tsvTemplate.copy(name = "metadata")
        OpenFiles.NEXTCLADE -> tsvTemplate.copy(name = "nextclade")
        OpenFiles.SEQUENCES -> fastaTemplate.copy(name = "sequences")
        OpenFiles.ALIGNED -> fastaTemplate.copy(name = "aligned")
        OpenFiles.TRANSLATION_E -> fastaTemplate.copy(name = "translation_E")
        OpenFiles.TRANSLATION_M -> fastaTemplate.copy(name = "translation_M")
        OpenFiles.TRANSLATION_N -> fastaTemplate.copy(name = "translation_N")
        OpenFiles.TRANSLATION_ORF1a -> fastaTemplate.copy(name = "translation_ORF1a")
        OpenFiles.TRANSLATION_ORF1b -> fastaTemplate.copy(name = "translation_ORF1b")
        OpenFiles.TRANSLATION_ORF3a -> fastaTemplate.copy(name = "translation_ORF3a")
        OpenFiles.TRANSLATION_ORF6 -> fastaTemplate.copy(name = "translation_ORF6")
        OpenFiles.TRANSLATION_ORF7a -> fastaTemplate.copy(name = "translation_ORF7a")
        OpenFiles.TRANSLATION_ORF7b -> fastaTemplate.copy(name = "translation_ORF7b")
        OpenFiles.TRANSLATION_ORF8 -> fastaTemplate.copy(name = "translation_ORF8")
        OpenFiles.TRANSLATION_ORF9b -> fastaTemplate.copy(name = "translation_ORF9b")
        OpenFiles.TRANSLATION_S -> fastaTemplate.copy(name = "translation_S")
    }
    val url = URL("https://data.nextstrain.org/files/ncov/open/${outputFile.filename}")
    url.openStream().use { input ->
        Files.copy(input, outputFile.path, StandardCopyOption.REPLACE_EXISTING)
    }
    return outputFile
}

private fun transformSourceFilesToNdjson(
    sourceFiles: Map<OpenFiles, File>,
    ndjsonPath: Path
): Map<OpenFiles, File> {
    val transformToNdjsonTasks = sourceFiles.map { (name, file) ->
        when (file.type) {
            FileType.TSV -> {
                { name to tsvToNdjson(file, ndjsonPath) }
            }

            FileType.FASTA -> {
                { name to fastaToNdjson("strain", file, ndjsonPath) }
            }

            else -> throw IllegalStateException()
        }
    }
    return runParallel(transformToNdjsonTasks, 4).toMap()
}

private fun sortNdjsonFiles(
    ndjsonFiles: Map<OpenFiles, File>,
    sortedPath: Path
): Map<OpenFiles, File> {
    val sortNdjsonTasks = ndjsonFiles.map { (name, file) ->
        {
            val sortBy = if (name == OpenFiles.NEXTCLADE) {
                "seqName"
            } else {
                "strain"
            }
            val subWorkdir = sortedPath.resolve("workdir_${file.name}")
            Files.createDirectories(subWorkdir)
            val sortedFile = sortNdjson(sortBy, subWorkdir, file, sortedPath)
            Files.walk(subWorkdir).sorted(Comparator.reverseOrder()).forEach(Files::delete)
            name to sortedFile
        }
    }
    return runParallel(sortNdjsonTasks, 2).toMap()
}

private fun joinFiles(
    sortedFiles: Map<OpenFiles, File>,
    joinedPath: Path
): File {
    return joinSC2NextstrainOpenData(
        sortedFiles[OpenFiles.METADATA]!!,
        sortedFiles[OpenFiles.NEXTCLADE]!!,
        sortedFiles[OpenFiles.SEQUENCES]!!,
        sortedFiles[OpenFiles.ALIGNED]!!,
        listOf(
            "E" to sortedFiles[OpenFiles.TRANSLATION_E]!!,
            "M" to sortedFiles[OpenFiles.TRANSLATION_M]!!,
            "N" to sortedFiles[OpenFiles.TRANSLATION_N]!!,
            "ORF1a" to sortedFiles[OpenFiles.TRANSLATION_ORF1a]!!,
            "ORF1b" to sortedFiles[OpenFiles.TRANSLATION_ORF1b]!!,
            "ORF3a" to sortedFiles[OpenFiles.TRANSLATION_ORF3a]!!,
            "ORF6" to sortedFiles[OpenFiles.TRANSLATION_ORF6]!!,
            "ORF7a" to sortedFiles[OpenFiles.TRANSLATION_ORF7a]!!,
            "ORF7b" to sortedFiles[OpenFiles.TRANSLATION_ORF7b]!!,
            "ORF8" to sortedFiles[OpenFiles.TRANSLATION_ORF8]!!,
            "ORF9b" to sortedFiles[OpenFiles.TRANSLATION_ORF9b]!!,
            "S" to sortedFiles[OpenFiles.TRANSLATION_S]!!
        ),
        joinedPath,
        "processed"
    )
}

private fun moveFinalFile(provisionFile: File, directoryPath: Path): File {
    val zoneId = ZoneId.systemDefault()
    val newDataVersion = Instant.now().atZone(zoneId).toEpochSecond()
    val finalProvisionFile = File(
        "provision.$newDataVersion",
        directoryPath,
        provisionFile.sorted,
        provisionFile.type,
        provisionFile.compression
    )
    renameFile(provisionFile.path, finalProvisionFile.path)
    return finalProvisionFile
}
