package org.genspectrum.ingest.proc

import org.apache.commons.csv.CSVFormat
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

fun tsvToNdjson(
    input: File,
    outputDirectory: Path = input.directory,
    outputName: String = input.name,
    outputCompression: Compression = input.compression
): File {
    require(input.type == FileType.TSV)
    val outputFile = File(outputName, outputDirectory, input.sorted, FileType.NDJSON, outputCompression)

    readFile(input.path).use { inputStream ->
        val records = CSVFormat.Builder.create(CSVFormat.TDF).build().parse(inputStream.reader())
        val iterator = records.iterator()

        // Headers
        val headerRecord = iterator.next()
        val headers = headerRecord.map { it }

        // Read, transform, and write data
        val writer = writeNdjson<Any>(writeFile(outputFile.path))
        iterator.forEachRemaining {
            val entry = mutableMapOf<String, String>()
            headers.withIndex().forEach { (index, header) -> entry[header] = it.get(index) }
            writer.write(entry)
        }
        writer.close()
    }

    return outputFile
}
