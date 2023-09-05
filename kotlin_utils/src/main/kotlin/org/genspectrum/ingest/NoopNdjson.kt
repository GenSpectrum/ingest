package org.genspectrum.ingest

import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class NoopNdjson {

    /**
     * This function just reads, decompresses, deserializes, serializes, compresses, and writes a ndjson.zst file
     * without performing any other processing. It is useful to measure the needed time for these basic tasks.
     */
    fun run(inputFile: Path, outputFile: Path) {
        val reader = readNdjson<MutableEntry>(readFile(inputFile))
        val writer = writeNdjson<MutableEntry>(writeFile(outputFile))
        var i = 0
        for (entry in reader) {
            writer.write(entry)
            i++
            if (i % 100000 == 0) {
                println(i)
            }
        }
        writer.close()
    }

}
