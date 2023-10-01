package org.genspectrum.ingest.workflows

import com.alibaba.fastjson2.JSONObject
import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.AlignedGenome
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
        val provisionFilePath = fromSourcePath.resolve("provision.ndjson.xz")
        downloadFromGisaid(provisionFilePath, url, user, password)

        val basicTransformedPath = workdir.resolve("02_basic_transformed")
        Files.createDirectories(basicTransformedPath)
        val provisionFile2Path = basicTransformedPath.resolve("provision.ndjson.zst")
        val hashesFilePath = basicTransformedPath.resolve("provision.hashes.ndjson.zst")
        transformSC2GisaidBasics(provisionFilePath, provisionFile2Path, hashesFilePath)

        val comparisonPath = workdir.resolve("03_comparison")
        Files.createDirectories(comparisonPath)
        val comparisonFilePath = comparisonPath.resolve("comparison.json.zst")
        val results = compareHashes(
            getHashEntries(previousHashes, "gisaidEpiIsl"),
            getHashEntries(hashesFilePath, "gisaidEpiIsl")
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
        val extractedFilePath = addedOrChangedPath.resolve("provision.ndjson.zst")
        val extractedSortedFilePath = addedOrChangedPath.resolve("provision.sorted.ndjson.zst")
        extractAddedOrChanged("gisaidEpiIsl", comparisonFilePath, provisionFile2Path, extractedFilePath)
        sortNdjson("id", extractedFilePath, extractedSortedFilePath, addedOrChangedWorkdirPath)
        Files.walk(addedOrChangedWorkdirPath).sorted(Comparator.reverseOrder()).forEach(Files::delete)

        val nextcladePath = workdir.resolve("05_nextclade")
        Files.createDirectories(nextcladePath)
        val sequencesFastaPath = nextcladePath.resolve("sequences.sorted.fasta.zst")
        unalignedNucleotideSequencesToFasta("gisaidEpiIsl", "main", extractedSortedFilePath, sequencesFastaPath)
        runNextclade(sequencesFastaPath, nextcladePath)
        val alignedFastaPath = nextcladePath.resolve("aligned.sorted.fasta.zst")
        val nextcladeTsvPath = nextcladePath.resolve("nextclade.sorted.tsv.zst")
        val alignedGenome = AlignedGenome.loadFromFile(Path("reference-genome.sc2.json"))
        val translationFastaPaths = alignedGenome.aminoAcidSequences.keys.map { it to nextcladePath.resolve("translation_$it.sorted.fasta.zst") }

        val nextcladeNdjsonPath = workdir.resolve("06_nextclade_ndjson")
        Files.createDirectories(nextcladeNdjsonPath)
        val nextcladeNdjsonFilePath = nextcladeNdjsonPath.resolve("nextclade.sorted.ndjson.zst")
        val alignedNdjsonFilePath = nextcladeNdjsonPath.resolve("aligned.sorted.ndjson.zst")
        val translationNdjsonPaths = translationFastaPaths.map {
            it.first to nextcladeNdjsonPath.resolve(it.second.toString().replace("fasta", "ndjson"))
        }
        val transformToNdjsonTasks = listOf(
            { tsvToNdjson(nextcladeTsvPath, nextcladeNdjsonFilePath) },
            { fastaToNdjson("id", alignedFastaPath, alignedNdjsonFilePath) }
        ) + translationFastaPaths.zip(translationNdjsonPaths).map {
            { fastaToNdjson("id", it.first.second, it.second.second) }
        }
        runParallel(transformToNdjsonTasks, 8)

        val joinedPath = workdir.resolve("07_joined")
        Files.createDirectories(joinedPath)
        val joinedFilePath = joinedPath.resolve("joined.sorted.ndjson.zst")
        joinSC2GisaidData(
            extractedSortedFilePath,
            nextcladeNdjsonFilePath,
            alignedNdjsonFilePath,
            translationNdjsonPaths,
            joinedFilePath
        )
    }

    private fun downloadFromGisaid(outputPath: Path, url: String, user: String, password: String) {
        val connection = URL(url).openConnection() as HttpURLConnection
        val auth = Base64.getEncoder().encodeToString("$user:$password".toByteArray())
        connection.setRequestProperty("Authorization", "Basic $auth")

        Files.newOutputStream(outputPath).use { outputStream ->
            connection.inputStream.copyTo(outputStream)
        }

        connection.disconnect()
    }

    private fun getHashEntries(path: Path, idColumn: String): List<HashEntry> {
        val reader = readNdjson<JSONObject>(readFile(path))
        return reader.map { HashEntry(it.getString(idColumn), it.getString("md5")) }.toList()
    }

    private fun runNextclade(inputFile: Path, outputDirectory: Path) {
        val command = arrayOf(
            "external_tools/nextclade",
            "run",
            "-d", "sars-cov-2",
            "-j", "16",
            "--in-order",
            "--output-tsv", "$outputDirectory/nextclade.sorted.tsv.zst",
            "--output-fasta", "$outputDirectory/aligned.sorted.fasta.zst",
            "--output-translations", "$outputDirectory/translation_{gene}.sorted.fasta.zst",
            inputFile.toString()
        )

        val process = ProcessBuilder(*command)
            .redirectError(outputDirectory.resolve("stderr.log").toFile())
            .redirectOutput(outputDirectory.resolve("stdout.log").toFile())
            .start()

        val exitCode = process.waitFor()

        if (exitCode != 0) {
            println("Nextclade execution failed with exit code: $exitCode")
        }
    }
}
