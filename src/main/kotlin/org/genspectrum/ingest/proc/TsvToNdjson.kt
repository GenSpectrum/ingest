package org.genspectrum.ingest.proc

import org.apache.commons.csv.CSVFormat
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

fun tsvToNdjson(input: Path, output: Path) {
    readFile(input).use { inputStream ->
        val records = CSVFormat.Builder.create(CSVFormat.TDF).build().parse(inputStream.reader())
        val iterator = records.iterator()

        // Headers
        val headerRecord = iterator.next()
        val headers = headerRecord.map { it }

        // Read, transform, and write data
        val writer = writeNdjson<Any>(writeFile(output))
        iterator.forEachRemaining {
            val entry = mutableMapOf<String, String>()
            headers.withIndex().forEach { (index, header) -> entry[header] = it.get(index) }
            writer.write(entry)
        }
        writer.close()
    }
}
