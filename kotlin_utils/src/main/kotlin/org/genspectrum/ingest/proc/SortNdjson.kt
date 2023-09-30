package org.genspectrum.ingest.proc

import com.alibaba.fastjson2.JSON
import com.alibaba.fastjson2.JSONObject
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.nio.file.Path


fun sortNdjson(sortBy: String, input: Path, output: Path, workdir: Path) {
    val sortKeys = getSortValues(sortBy, input)
    val buckets = splitEntriesIntoBuckets(sortKeys, 100000)
    val smallFiles = splitIntoSmallFiles(sortBy, input, workdir, buckets)
    val sortedSmallFiles = smallFiles.map { sortSmallFile(sortBy, it) }
    mergeSmallFiles(sortedSmallFiles, output)
}

private fun findBucket(sampleName: String, buckets: List<Bucket>): Int? {
    for ((index, bucket) in buckets.withIndex()) {
        if (bucket.minName <= sampleName && sampleName <= bucket.maxName) {
            return index
        }
    }
    return null
}

private fun getSortValues(sortBy: String, file: Path): List<String> {
    return readNdjson<JSONObject>(readFile(file)).map { it.getString(sortBy) }.toList()
}

private fun splitEntriesIntoBuckets(sortKeys: List<String>, maxSizePerBucket: Int): List<Bucket> {
    val sorted = sortKeys.sorted()
    val buckets = mutableListOf<Bucket>()
    var start: String? = null
    var i = 0

    for (current in sorted) {
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
        buckets.add(Bucket(start, sorted.last()))
    }

    return buckets
}

private fun splitIntoSmallFiles(sortBy: String, file: Path, workdir: Path, buckets: List<Bucket>): List<Path> {
    if (buckets.isEmpty()) {
        return emptyList()
    }
    val smallFilePaths = List(buckets.size) { index -> workdir.resolve("small_${index}.zst") }
    val outputStreams = smallFilePaths.map { writeFile(it) }

    BufferedReader(InputStreamReader(readFile(file))).use { reader ->
        reader.forEachLine { line ->
            val sortKey = JSON.parseObject(line).getString(sortBy)
            val bucketIndex = findBucket(sortKey, buckets)!!
            val currentStream = outputStreams[bucketIndex]
            currentStream.write(line.toByteArray())
            currentStream.write("\n".toByteArray())
        }
    }

    outputStreams.forEach { it.close() }
    return smallFilePaths
}

private fun sortSmallFile(sortBy: String, smallFile: Path): Path {
    val outputFile = smallFile.resolveSibling("sorted_${smallFile.fileName}")
    val entries = mutableListOf<Pair<String, String>>()
    BufferedReader(InputStreamReader(readFile(smallFile))).forEachLine { line ->
        val sortKey = JSON.parseObject(line).getString(sortBy)
        entries.add(Pair(sortKey, line))
    }
    val sorted = entries.sortedBy { it.first }
    writeFile(outputFile).use { outputStream ->
        sorted.forEach {
            outputStream.write(it.second.toByteArray())
            outputStream.write("\n".toByteArray())
        }
    }
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

private data class Bucket(
    val minName: String,
    val maxName: String,
)
