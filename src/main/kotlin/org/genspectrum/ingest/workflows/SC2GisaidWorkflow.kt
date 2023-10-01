package org.genspectrum.ingest.workflows

import com.alibaba.fastjson2.JSONObject
import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.AlignedGenome
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.proc.*
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.runParallel
import org.genspectrum.ingest.utils.writeFile
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.io.path.Path

class SC2GisaidWorkflow {

    fun run(
        workdir: Path,
        url: String,
        user: String,
        password: String,
        previousProcessed: Path,
        previousHashes: Path
    ) {
        val fromSourcePath = workdir.resolve("01_from_source")
        Files.createDirectories(fromSourcePath)
        val sourceFile = downloadFromGisaid(url, user, password, fromSourcePath, "provision")

        val basicTransformedPath = workdir.resolve("02_basic_transformed")
        Files.createDirectories(basicTransformedPath)
        val tmp = transformSC2GisaidBasics(sourceFile, basicTransformedPath)
        val transformedFile = tmp.dataFile
        val hashesFile = tmp.hashesFile

        val comparisonPath = workdir.resolve("03_comparison")
        Files.createDirectories(comparisonPath)
        val comparisonFilePath = comparisonPath.resolve("comparison.json.zst")
        val results = compareHashes(
            getHashEntries(previousHashes, "gisaidEpiIsl"),
            getHashEntries(hashesFile.path, "gisaidEpiIsl")
        )
        println("Added: ${results.added.size}, changed: ${results.changed.size}, deleted: ${results.deleted.size}, unchanged: ${results.unchanged.size}")
        writeFile(comparisonFilePath).use { outputStream ->
            outputStream.write(results.toJSONByteArray(JSONWriter.Feature.LargeObject))
            outputStream.write("\n".toByteArray())
        }

        val addedOrChangedPath = workdir.resolve("04_added_or_changed")
        val addedOrChangedWorkdirPath = addedOrChangedPath.resolve("workdir")
        Files.createDirectories(addedOrChangedPath)
        Files.createDirectories(addedOrChangedWorkdirPath)
        val extractedFile = extractAddedOrChanged("gisaidEpiIsl", comparisonFilePath, transformedFile, addedOrChangedPath)
        val extractedSortedFile = sortNdjson("id", addedOrChangedWorkdirPath, extractedFile)
        Files.walk(addedOrChangedWorkdirPath).sorted(Comparator.reverseOrder()).forEach(Files::delete)

        val nextcladePath = workdir.resolve("05_nextclade")
        Files.createDirectories(nextcladePath)
        val sequencesFasta = unalignedNucleotideSequencesToFasta(
            "gisaidEpiIsl", "main", extractedSortedFile, nextcladePath, "sequences")
        val nextcladeFiles = runNextclade(sequencesFasta, nextcladePath)

        val nextcladeNdjsonPath = workdir.resolve("06_nextclade_ndjson")
        Files.createDirectories(nextcladeNdjsonPath)
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

        val joinedPath = workdir.resolve("07_joined")
        Files.createDirectories(joinedPath)
        val joinedFile = joinSC2GisaidData(
            extractedSortedFile,
            nextcladeNdjsonFiles.nextclade,
            nextcladeNdjsonFiles.aligned,
            nextcladeNdjsonFiles.translations.toList(),
            joinedPath,
            "joined"
        )
    }

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

    private fun getHashEntries(path: Path, idColumn: String): List<HashEntry> {
        val reader = readNdjson<JSONObject>(readFile(path))
        return reader.map { HashEntry(it.getString(idColumn), it.getString("md5")) }.toList()
    }

    private fun runNextclade(inputFile: File, outputDirectory: Path): NextcladeOutput {
        require(inputFile.type == FileType.FASTA)
        val alignedGenome = AlignedGenome.loadFromFile(Path("reference-genome.sc2.json"))
        val output = NextcladeOutput(
            File("nextclade", outputDirectory, inputFile.sorted, FileType.TSV, Compression.ZSTD),
            File("aligned", outputDirectory, inputFile.sorted, FileType.FASTA, Compression.ZSTD),
            alignedGenome.aminoAcidSequences.keys.associateWith {
                File("translation_$it", outputDirectory, inputFile.sorted, FileType.FASTA, Compression.ZSTD)
            }
        )

        val command = arrayOf(
            "./external_tools/nextclade",
            "run",
            "-d", "sars-cov-2",
            "-j", "16",
            "--in-order",
            "--output-tsv", output.nextclade.path.toString(),
            "--output-fasta", output.aligned.path.toString(),
            "--output-translations", "$outputDirectory/translation_{gene}${if (inputFile.sorted) ".sorted" else ""}.fasta.zst",
            inputFile.path.toString()
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
}

private data class NextcladeOutput (
    val nextclade: File,
    val aligned: File,
    val translations: Map<String, File>
)
