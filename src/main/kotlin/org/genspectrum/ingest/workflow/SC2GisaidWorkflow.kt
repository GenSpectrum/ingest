package org.genspectrum.ingest.workflow

import com.alibaba.fastjson2.JSON
import com.alibaba.fastjson2.JSONObject
import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.AlignedGenome
import org.genspectrum.ingest.file.AllPangoLineagesFile
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.proc.HashEntry
import org.genspectrum.ingest.proc.compareHashes
import org.genspectrum.ingest.proc.concatFiles
import org.genspectrum.ingest.proc.extractAddedOrChanged
import org.genspectrum.ingest.proc.extractUnchanged
import org.genspectrum.ingest.proc.fastaToNdjson
import org.genspectrum.ingest.proc.joinSC2GisaidData
import org.genspectrum.ingest.proc.renameFile
import org.genspectrum.ingest.proc.sortNdjson
import org.genspectrum.ingest.proc.transformSC2GisaidBasics
import org.genspectrum.ingest.proc.tsvToNdjson
import org.genspectrum.ingest.proc.unalignedNucleotideSequencesToFasta
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.readNdjson
import org.genspectrum.ingest.util.runParallel
import org.genspectrum.ingest.util.writeFile
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Base64
import kotlin.io.path.Path

fun runSC2GisaidWorkflow(
    workdir: Path,
    url: String,
    user: String,
    password: String,
    previousProcessed: File,
    previousAllPangoLineagesFile: AllPangoLineagesFile,
    previousHashes: Path,
    geoLocationRulesFile: Path
) {
    val fromSourcePath = workdir.resolve("01_from_source")
    Files.createDirectories(fromSourcePath)
    val sourceFile = downloadFromGisaid(url, user, password, fromSourcePath, "provision")

    println("${LocalDateTime.now()}: Finished downloading from GISAID")

    val basicTransformedPath = workdir.resolve("02_basic_transformed")
    Files.createDirectories(basicTransformedPath)
    val (transformedFile, hashesFile) = transformAndHash(sourceFile, basicTransformedPath, geoLocationRulesFile)

    println("${LocalDateTime.now()}: Finished transformAndHash")

    val comparisonPath = workdir.resolve("03_comparison")
    Files.createDirectories(comparisonPath)
    val comparisonFilePath = compareHashes(comparisonPath, previousHashes, hashesFile)

    println("${LocalDateTime.now()}: Finished compareHashes")

    val addedOrChangedPath = workdir.resolve("04_added_or_changed")
    Files.createDirectories(addedOrChangedPath)
    val extractedSortedFile = extractChangedEntries(addedOrChangedPath, comparisonFilePath, transformedFile)

    println("${LocalDateTime.now()}: Finished extractChangedEntries")

    val nextcladePath = workdir.resolve("05_nextclade")
    Files.createDirectories(nextcladePath)
    val nextcladeFiles = runNextclade(extractedSortedFile, nextcladePath)

    println("${LocalDateTime.now()}: Finished runNextclade")

    val nextcladeDatasetVersion = getNextcladeDatasetVersion()

    println("${LocalDateTime.now()}: Finished getNextcladeDatasetVersion")

    val nextcladeNdjsonPath = workdir.resolve("06_nextclade_ndjson")
    Files.createDirectories(nextcladeNdjsonPath)
    val nextcladeNdjsonFiles = transformNextcladeOutputToNdjson(nextcladeFiles, nextcladeNdjsonPath)

    println("${LocalDateTime.now()}: Finished transformNextcladeOutputToNdjson")

    val joinedPath = workdir.resolve("07_joined")
    Files.createDirectories(joinedPath)
    val (joinedFilePath, newPangoLineagesFile) = joinFiles(
        extractedSortedFile,
        nextcladeNdjsonFiles,
        joinedPath,
        nextcladeDatasetVersion
    )

    println("${LocalDateTime.now()}: Finished joinFiles")

    val unchangedPath = workdir.resolve("08_unchanged")
    Files.createDirectories(unchangedPath)
    val unchangedFilePath = extractUnchangedEntries(unchangedPath, previousProcessed, comparisonFilePath)

    println("${LocalDateTime.now()}: Finished extractUnchangedEntries")

    val unchangedAndNewPath = workdir.resolve("09_unchanged_and_new")
    Files.createDirectories(unchangedAndNewPath)
    val unchangedAndNewFilePath = mergeUnchangedAndNew(
        outputDirectory = unchangedAndNewPath,
        unchangedFilePath = unchangedFilePath,
        joinedFilePath = joinedFilePath
    )
    val allPangoLineagesFile = mergePangoLineageFiles(
        outputDirectory = unchangedAndNewPath,
        previousAllPangoLineagesFile = previousAllPangoLineagesFile,
        newPangoLineagesFile = newPangoLineagesFile
    )

    println("${LocalDateTime.now()}: Finished mergeUnchangedAndNew")

    val finalDestinationPath = workdir.resolve("00_archive")
    Files.createDirectories(finalDestinationPath)
    val (finalHashesFile, finalProvisionFile) = moveFinalFiles(
        hashesFile = hashesFile,
        provisionFile = unchangedAndNewFilePath,
        allPangoLineagesFile = allPangoLineagesFile,
        directoryPath = finalDestinationPath
    )

    println("Final output: ${finalHashesFile.path}, ${finalProvisionFile.path}")
}

private data class NextcladeOutput(
    val nextclade: File,
    val aligned: File,
    val translations: Map<String, File>
)

private fun downloadFromGisaid(
    url: String,
    user: String,
    password: String,
    outputDirectory: Path,
    outputName: String
): File {
    val outputFile = File(outputName, outputDirectory, false, FileType.NDJSON, Compression.XZ)

    val connection = URL(url).openConnection() as HttpURLConnection
    val auth = Base64.getEncoder().encodeToString("$user:$password".toByteArray())
    connection.setRequestProperty("Authorization", "Basic $auth")
    Files.newOutputStream(outputFile.path).use { outputStream ->
        connection.inputStream.copyTo(outputStream)
    }
    connection.disconnect()

    return outputFile
}

private fun transformAndHash(
    sourceFile: File,
    basicTransformedPath: Path,
    geoLocationRulesFile: Path
): Pair<File, File> {
    val tmp = transformSC2GisaidBasics(sourceFile, basicTransformedPath, geoLocationRulesFile)
    return Pair(tmp.dataFile, tmp.hashesFile)
}

private fun compareHashes(
    comparisonPath: Path,
    previousHashes: Path,
    hashesFile: File
): Path {
    val comparisonFilePath = comparisonPath.resolve("comparison.json.zst")
    val results = compareHashes(
        getHashEntries(previousHashes, "gisaidEpiIsl"),
        getHashEntries(hashesFile.path, "gisaidEpiIsl")
    )
    writeFile(comparisonFilePath).use { outputStream ->
        outputStream.write(results.toJSONByteArray(JSONWriter.Feature.LargeObject))
        outputStream.write("\n".toByteArray())
    }
    return comparisonFilePath
}

private fun getHashEntries(path: Path, idColumn: String): List<HashEntry> {
    val reader = readNdjson<JSONObject>(readFile(path))
    return reader.map { HashEntry(it.getString(idColumn), it.getString("md5")) }.toList()
}

private fun extractChangedEntries(
    addedOrChangedPath: Path,
    comparisonFilePath: Path,
    transformedFile: File
): File {
    val addedOrChangedWorkdirPath = addedOrChangedPath.resolve("workdir")
    Files.createDirectories(addedOrChangedWorkdirPath)
    val extractedFile =
        extractAddedOrChanged("gisaidEpiIsl", comparisonFilePath, transformedFile, addedOrChangedPath)
    val extractedSortedFile = sortNdjson("id", addedOrChangedWorkdirPath, extractedFile)
    Files.walk(addedOrChangedWorkdirPath).sorted(Comparator.reverseOrder()).forEach(Files::delete)
    return extractedSortedFile
}

private fun runNextclade(
    extractedSortedFile: File,
    outputDirectory: Path
): NextcladeOutput {
    val sequencesFasta = unalignedNucleotideSequencesToFasta(
        "gisaidEpiIsl", "main", extractedSortedFile, outputDirectory, "sequences"
    )
    val alignedGenome = AlignedGenome.loadFromFile(Path("reference-genome.sc2.json"))
    val output = NextcladeOutput(
        File("nextclade", outputDirectory, sequencesFasta.sorted, FileType.TSV, Compression.ZSTD),
        File("aligned", outputDirectory, sequencesFasta.sorted, FileType.FASTA, Compression.ZSTD),
        alignedGenome.aminoAcidSequences.keys.associateWith {
            File("translation_$it", outputDirectory, sequencesFasta.sorted, FileType.FASTA, Compression.ZSTD)
        }
    )

    val command = arrayOf(
        "./external_tools/nextclade",
        "run",
        "-d",
        "sars-cov-2",
        "-j",
        "16",
        "--in-order",
        "--output-tsv",
        output.nextclade.path.toString(),
        "--output-fasta",
        output.aligned.path.toString(),
        "--output-translations",
        "$outputDirectory/translation_{cds}${if (sequencesFasta.sorted) ".sorted" else ""}.fasta.zst",
        sequencesFasta.path.toString()
    )

    val process = ProcessBuilder(*command)
        .redirectError(outputDirectory.resolve("stderr.log").toFile())
        .redirectOutput(outputDirectory.resolve("stdout.log").toFile())
        .start()

    val exitCode = process.waitFor()

    if (exitCode != 0) {
        println("Nextclade execution failed with exit code: $exitCode")
    }

    return output
}

private fun getNextcladeDatasetVersion(): String {
    val command = arrayOf(
        "./external_tools/nextclade",
        "dataset",
        "list",
        "-n", "sars-cov-2",
        "--json"
    )

    val process = ProcessBuilder(*command).start()
    val reader = BufferedReader(InputStreamReader(process.inputStream))
    val output = reader.readText()
    reader.close()

    return JSON.parseArray(output)
        .getJSONObject(0)
        .getJSONObject("version")
        .getString("tag")
}

private fun transformNextcladeOutputToNdjson(
    nextcladeFiles: NextcladeOutput,
    nextcladeNdjsonPath: Path
): NextcladeOutput {
    val translationNames = nextcladeFiles.translations.keys.toList()
    val transformToNdjsonTasks = listOf(
        { tsvToNdjson(nextcladeFiles.nextclade, nextcladeNdjsonPath) },
        { fastaToNdjson("id", nextcladeFiles.aligned, nextcladeNdjsonPath) }
    ) + translationNames.map {
        { fastaToNdjson("id", nextcladeFiles.translations[it]!!, nextcladeNdjsonPath) }
    }
    val tmp2 = runParallel(transformToNdjsonTasks, 8)
    val nextcladeNdjsonFiles = NextcladeOutput(
        tmp2[0], tmp2[1],
        translationNames.withIndex().map { (index, name) -> name to tmp2[2 + index] }.toMap()
    )
    return nextcladeNdjsonFiles
}

private fun joinFiles(
    extractedSortedFile: File,
    nextcladeNdjsonFiles: NextcladeOutput,
    joinedPath: Path,
    nextcladeDatasetVersion: String,
): Pair<File, AllPangoLineagesFile> {
    return joinSC2GisaidData(
        provisionFile = extractedSortedFile,
        nextcladeFile = nextcladeNdjsonFiles.nextclade,
        alignedFile = nextcladeNdjsonFiles.aligned,
        translationFiles = nextcladeNdjsonFiles.translations.toList(),
        outputDirectory = joinedPath,
        outputName = "joined",
        nextcladeDatasetVersion = nextcladeDatasetVersion,
    )
}

fun extractUnchangedEntries(unchangedPath: Path, previousProcessed: File, comparisonFilePath: Path): File {
    return extractUnchanged("gisaidEpiIsl", comparisonFilePath, previousProcessed, unchangedPath)
}

fun mergeUnchangedAndNew(outputDirectory: Path, unchangedFilePath: File, joinedFilePath: File): File {
    val outputFile = File("merged", outputDirectory, false, FileType.NDJSON, Compression.ZSTD)
    concatFiles(arrayOf(unchangedFilePath.path, joinedFilePath.path), outputFile.path)
    return outputFile
}

fun mergePangoLineageFiles(
    outputDirectory: Path,
    previousAllPangoLineagesFile: AllPangoLineagesFile,
    newPangoLineagesFile: AllPangoLineagesFile
): AllPangoLineagesFile {
    val previousAllPangoLineages = previousAllPangoLineagesFile.read()
    val newPangoLineages = newPangoLineagesFile.read()
    val allPangoLineages = previousAllPangoLineages + newPangoLineages

    val outputFile = AllPangoLineagesFile(directory = outputDirectory)
    outputFile.write(allPangoLineages)
    return outputFile
}

private fun moveFinalFiles(
    hashesFile: File,
    provisionFile: File,
    allPangoLineagesFile: AllPangoLineagesFile,
    directoryPath: Path
): Pair<File, File> {
    val zoneId = ZoneId.systemDefault()
    val newDataVersion = Instant.now().atZone(zoneId).toEpochSecond()
    val dataVersionPath = directoryPath.resolve(newDataVersion.toString())
    Files.createDirectories(dataVersionPath)

    val finalHashesFile = File(
        "provision.$newDataVersion.hashes",
        dataVersionPath,
        hashesFile.sorted,
        hashesFile.type,
        hashesFile.compression
    )
    val finalProvisionFile = File(
        "provision.$newDataVersion",
        dataVersionPath,
        provisionFile.sorted,
        provisionFile.type,
        provisionFile.compression
    )
    val finalPangoLineagesFile = AllPangoLineagesFile(
        dataVersion = newDataVersion.toString(),
        directory = dataVersionPath
    )

    renameFile(oldPath = hashesFile.path, newPath = finalHashesFile.path)
    renameFile(oldPath = provisionFile.path, newPath = finalProvisionFile.path)
    renameFile(oldPath = allPangoLineagesFile.path, newPath = finalPangoLineagesFile.path)
    return Pair(finalHashesFile, finalProvisionFile)
}
