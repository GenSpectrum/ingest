package org.genspectrum.ingest.workflows

import org.genspectrum.ingest.FastaToNdjson
import org.genspectrum.ingest.JoinSC2NextstrainOpenData
import org.genspectrum.ingest.SortNdjson
import org.genspectrum.ingest.TsvToNdjson
import org.genspectrum.ingest.utils.runParallel
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.LocalDateTime

class SC2NextstrainOpenWorkflow {

    fun run(workdir: Path) {
        val fromSourcePath = workdir.resolve("01_from_source")
        Files.createDirectories(fromSourcePath)
        runParallel(allFilesOriginalEndings.map { { downloadFromNextstrain(it, fromSourcePath) } }, 3)

        val ndjsonPath = workdir.resolve("02_ndjson")
        Files.createDirectories(ndjsonPath)
        val transformToNdjsonTasks = allFilesOriginalEndings.zip(allFilesNdjsonEndings)
            .map { (original, ndjson) ->
                {
                    if (original.endsWith(".tsv.zst")) {
                        TsvToNdjson().run(fromSourcePath.resolve(original), ndjsonPath.resolve(ndjson))
                    } else {
                        FastaToNdjson().run("strain", fromSourcePath.resolve(original), ndjsonPath.resolve(ndjson))
                    }
                }
            }
        runParallel(transformToNdjsonTasks, 4)

        val sortedPath = workdir.resolve("03_sorted")
        Files.createDirectories(sortedPath)
        val sortNdjsonTasks = allFilesNdjsonEndings
            .map { file ->
                {
                    val sortBy = if (file.startsWith("nextclade")) {
                        "seqName"
                    } else {
                        "strain"
                    }
                    val subWorkdir = sortedPath.resolve("workdir_$file")
                    Files.createDirectories(subWorkdir)
                    SortNdjson().run(sortBy, ndjsonPath.resolve(file), sortedPath.resolve(file), subWorkdir)
                    Files.walk(subWorkdir).sorted(Comparator.reverseOrder()).forEach(Files::delete)
                }
            }
        runParallel(sortNdjsonTasks, 2)

        val joinedPath = workdir.resolve("04_joined_and_cleaned")
        Files.createDirectories(joinedPath)
        JoinSC2NextstrainOpenData().run(
            sortedPath.resolve(OpenFiles.metadata + ".ndjson.zst"),
            sortedPath.resolve(OpenFiles.nextclade + ".ndjson.zst"),
            sortedPath.resolve(OpenFiles.sequences + ".ndjson.zst"),
            sortedPath.resolve(OpenFiles.aligned + ".ndjson.zst"),
            listOf(
                "E" to sortedPath.resolve(OpenFiles.translationE + ".ndjson.zst"),
                "M" to sortedPath.resolve(OpenFiles.translationM + ".ndjson.zst"),
                "N" to sortedPath.resolve(OpenFiles.translationN + ".ndjson.zst"),
                "ORF1a" to sortedPath.resolve(OpenFiles.translationORF1a + ".ndjson.zst"),
                "ORF1b" to sortedPath.resolve(OpenFiles.translationORF1b + ".ndjson.zst"),
                "ORF3a" to sortedPath.resolve(OpenFiles.translationORF3a + ".ndjson.zst"),
                "ORF6" to sortedPath.resolve(OpenFiles.translationORF6 + ".ndjson.zst"),
                "ORF7a" to sortedPath.resolve(OpenFiles.translationORF7a + ".ndjson.zst"),
                "ORF7b" to sortedPath.resolve(OpenFiles.translationORF7b + ".ndjson.zst"),
                "ORF8" to sortedPath.resolve(OpenFiles.translationORF8 + ".ndjson.zst"),
                "ORF9b" to sortedPath.resolve(OpenFiles.translationORF9b + ".ndjson.zst"),
                "S" to sortedPath.resolve(OpenFiles.translationS + ".ndjson.zst")
            ),
            joinedPath.resolve("processed.ndjson.zst")
        )
    }

    private fun downloadFromNextstrain(filename: String, outputDirectory: Path) {
        val url = URL("https://data.nextstrain.org/files/ncov/open/$filename")
        val outputPath = outputDirectory.resolve(filename)

        println("${LocalDateTime.now()} Start downloading $url")
        url.openStream().use { input ->
            Files.copy(input, outputPath, StandardCopyOption.REPLACE_EXISTING)
        }
        println("${LocalDateTime.now()} Finished downloading $url")
    }

}

private object OpenFiles {
    const val metadata = "metadata"
    const val nextclade = "nextclade"
    const val sequences = "sequences"
    const val aligned = "aligned"
    const val translationE = "translation_E"
    const val translationM = "translation_M"
    const val translationN = "translation_N"
    const val translationORF1a = "translation_ORF1a"
    const val translationORF1b = "translation_ORF1b"
    const val translationORF3a = "translation_ORF3a"
    const val translationORF6 = "translation_ORF6"
    const val translationORF7a = "translation_ORF7a"
    const val translationORF7b = "translation_ORF7b"
    const val translationORF8 = "translation_ORF8"
    const val translationORF9b = "translation_ORF9b"
    const val translationS = "translation_S"
}

private val tsvFiles = listOf(OpenFiles.metadata, OpenFiles.nextclade)

private val nucleotideSequenceFiles = listOf(OpenFiles.sequences, OpenFiles.aligned)

private val translationFiles = listOf(
    OpenFiles.translationE, OpenFiles.translationM, OpenFiles.translationN, OpenFiles.translationORF1a,
    OpenFiles.translationORF1b, OpenFiles.translationORF3a, OpenFiles.translationORF6, OpenFiles.translationORF7a,
    OpenFiles.translationORF7b, OpenFiles.translationORF8, OpenFiles.translationORF9b, OpenFiles.translationS
)

val allFilesOriginalEndings = tsvFiles.map { "${it}.tsv.zst" } +
        (nucleotideSequenceFiles + translationFiles).map { "${it}.fasta.zst" }

val allFilesNdjsonEndings = (tsvFiles + nucleotideSequenceFiles + translationFiles).map { "${it}.ndjson.zst" }
