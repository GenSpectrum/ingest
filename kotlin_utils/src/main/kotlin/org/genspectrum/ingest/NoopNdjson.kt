package org.genspectrum.ingest

import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.utils.EntryNdjsonReader
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import java.nio.file.Path

class NoopNdjson {

    /**
     * This function just reads, decompresses, deserializes, serializes, compresses, and writes a ndjson.zst file
     * without performing any other processing. It is useful to measure the needed time for these basic tasks.
     */
    fun run(inputFile: Path, outputFile: Path) {
        EntryNdjsonReader(readFile(inputFile)).use { reader ->
            writeFile(outputFile).use { outputStream ->
                for (json in reader) {
                    outputStream.write(json.toJSONByteArray(JSONWriter.Feature.WriteNulls))
                    outputStream.write("\n".toByteArray())
                }
            }
        }
    }

}