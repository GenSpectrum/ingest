package org.genspectrum.ingest.workflow

import com.alibaba.fastjson2.JSON
import com.alibaba.fastjson2.JSONObject
import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.AlignedGenome
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.proc.*
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
import java.util.*
import kotlin.io.path.Path

fun runSC2GisaidWorkflow(
    workdir: Path,
    url: String,
    user: String,
    password: String,
    previousProcessed: File,
    previousHashes: Path
) {
    val fromSourcePath = workdir.resolve("01_from_source")
    Files.createDirectories(fromSourcePath)
    val sourceFile = downloadFromGisaid(url, user, password, fromSourcePath, "provision")

    val basicTransformedPath = workdir.resolve("02_basic_transformed")
    Files.createDirectories(basicTransformedPath)
    val (transformedFile, hashesFile) = transformAndHash(sourceFile, basicTransformedPath)

    val comparisonPath = workdir.resolve("03_comparison")
    Files.createDirectories(comparisonPath)
    val comparisonFilePath = compareHashes(comparisonPath, previousHashes, hashesFile)

    val addedOrChangedPath = workdir.resolve("04_added_or_changed")
    Files.createDirectories(addedOrChangedPath)
    val extractedSortedFile = extractChangedEntries(addedOrChangedPath, comparisonFilePath, transformedFile)

    val nextcladePath = workdir.resolve("05_nextclade")
    Files.createDirectories(nextcladePath)
    val nextcladeFiles = runNextclade(extractedSortedFile, nextcladePath)

    val nextcladeDatasetVersion = getNextcladeDatasetVersion()

    val nextcladeNdjsonPath = workdir.resolve("06_nextclade_ndjson")
    Files.createDirectories(nextcladeNdjsonPath)
    val nextcladeNdjsonFiles = transformNextcladeOutputToNdjson(nextcladeFiles, nextcladeNdjsonPath)

    val joinedPath = workdir.resolve("07_joined")
    Files.createDirectories(joinedPath)
    val joinedFilePath = joinFiles(extractedSortedFile, nextcladeNdjsonFiles, joinedPath, nextcladeDatasetVersion)

    val unchangedPath = workdir.resolve("08_unchanged")
    Files.createDirectories(unchangedPath)
    val unchangedFilePath = extractUnchangedEntries(unchangedPath, previousProcessed, comparisonFilePath)

    val unchangedAndNewPath = workdir.resolve("09_unchanged_and_new")
    Files.createDirectories(unchangedAndNewPath)
    val unchangedAndNewFilePath = mergeUnchangedAndNew(unchangedAndNewPath, unchangedFilePath, joinedFilePath)

    // The final files are: hashesFile and unchangedAndNewFilePath
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
    basicTransformedPath: Path
): Pair<File, File> {
    val tmp = transformSC2GisaidBasics(sourceFile, basicTransformedPath)
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
        "$outputDirectory/translation_{gene}${if (sequencesFasta.sorted) ".sorted" else ""}.fasta.zst",
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
        .getJSONObject("attributes")
        .getJSONObject("tag")
        .getString("value")
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
): File {
    return joinSC2GisaidData(
        extractedSortedFile,
        nextcladeNdjsonFiles.nextclade,
        nextcladeNdjsonFiles.aligned,
        nextcladeNdjsonFiles.translations.toList(),
        joinedPath,
        "joined",
        nextcladeDatasetVersion,
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
