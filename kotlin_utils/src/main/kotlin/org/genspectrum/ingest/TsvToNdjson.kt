package org.genspectrum.ingest

import com.alibaba.fastjson2.toJSONByteArray
import org.apache.commons.csv.CSVFormat
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import java.nio.file.Path

class TsvToNdjson {

    fun run(input: Path, output: Path) {
        readFile(input).use { inputStream ->
            val records = CSVFormat.Builder.create(CSVFormat.TDF).build().parse(inputStream.reader())
            val iterator = records.iterator()

            // Headers
            val headerRecord = iterator.next()
            val headers = headerRecord.map { it }

            // Read, transform, and write data
            writeFile(output).use { outputStream ->
                iterator.forEachRemaining {
                    val entry = mutableMapOf<String, String>()
                    headers.withIndex().forEach{ (index, header) -> entry[header] = it.get(index)}
                    outputStream.write(entry.toJSONByteArray())
                    outputStream.write("\n".toByteArray())
                }
            }
        }
    }

}
