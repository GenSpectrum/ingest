package org.genspectrum.ingest

import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import org.genspectrum.ingest.utils.FastaReader
import java.io.*
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.absolutePathString

class FastaToSortedNdjson {

    fun run(input: Path, output: Path, workdir: Path) {
        val sampleNames = getSampleNames(input)
        val buckets = splitSamplesIntoBuckets(sampleNames, 300000)
        val smallFiles = splitIntoSmallFiles(input, workdir, buckets)
        val sortedSmallFiles = smallFiles.map { sortSmallFile(it) }
        mergeSmallFiles(sortedSmallFiles, output)
    }

    private fun readFile(file: Path): InputStream {
        return ZstdInputStream(FileInputStream(file.toFile()))
    }

    private fun writeFile(file: Path): OutputStream {
        val fileOut = FileOutputStream(file.toFile())
        val zstdOut = ZstdOutputStream(fileOut)
        return BufferedOutputStream(zstdOut)
    }

    private fun findBucket(sampleName: String, buckets: List<Bucket>): Int? {
        for ((index, bucket) in buckets.withIndex()) {
            if (bucket.minName <= sampleName && sampleName <= bucket.maxName) {
                return index
            }
        }
        return null
    }

    private fun getSampleNames(file: Path): List<String> {
        FastaReader(readFile(file)).use { reader ->
            return reader.map { it.sampleName }
        }
    }

    private fun splitSamplesIntoBuckets(sampleNames: List<String>, maxSizePerBucket: Int): List<Bucket> {
        val sortedSampleNames = sampleNames.sorted()
        val buckets = mutableListOf<Bucket>()
        var start: String? = null
        var i = 0

        for (current in sortedSampleNames) {
            if (start == null) {
                start = current
            }
            i++

            if (i >= maxSizePerBucket) {
                buckets.add(Bucket(start, current))
                start = null
                i = 0
            }
        }

        if (start != null) {
            buckets.add(Bucket(start, sortedSampleNames.last()))
        }

        return buckets
    }

    private fun splitIntoSmallFiles(file: Path, workdir: Path, buckets: List<Bucket>): List<Path> {
        if (buckets.isEmpty()) {
            return emptyList()
        }
        val smallFilePaths = List(buckets.size) { index -> workdir.resolve("small_${index}.fasta.zst") }
        val outputStreams = smallFilePaths.map { writeFile(it) }

        BufferedReader(InputStreamReader(readFile(file))).use { reader ->
            var currentStream: OutputStream? = null
            reader.forEachLine { line ->
                if (line.startsWith(">")) {
                    val sampleName = line.substring(1)
                    val bucketIndex = findBucket(sampleName, buckets)!!
                    currentStream = outputStreams[bucketIndex]
                }
                currentStream!!.write(line.toByteArray())
                currentStream!!.write("\n".toByteArray())
            }
        }

        outputStreams.forEach { it.close() }
        return smallFilePaths
    }

    private fun sortSmallFile(smallFile: Path): Path {
        val outputFile = Path("${smallFile.absolutePathString()}.sorted")
        val allSequences = FastaReader(readFile(smallFile)).toList()
        val sorted = allSequences.sortedBy { it.sampleName }
        writeFile(outputFile).use { outputStream -> sorted.forEach { it.writeToStream(outputStream) } }
        return outputFile
    }

    private fun mergeSmallFiles(smallFiles: List<Path>, outputFile: Path) {
        FileOutputStream(outputFile.toFile()).channel.use { outputChannel ->
            smallFiles.forEach { smallFile ->
                FileInputStream(smallFile.toFile()).channel.use { inputChannel ->
                    outputChannel.transferFrom(inputChannel, outputChannel.size(), inputChannel.size())
                }
            }
        }
    }
}

private data class Bucket(
    val minName: String,
    val maxName: String,
)
