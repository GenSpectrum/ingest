package org.genspectrum.ingest.proc

import org.genspectrum.ingest.entry.MutableEntry
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.readNdjson
import org.genspectrum.ingest.util.writeFile
import org.genspectrum.ingest.util.writeNdjson
import java.nio.file.Path

/**
 * This function just reads, decompresses, deserializes, serializes, compresses, and writes a ndjson.zst file
 * without performing any other processing. It is useful to measure the needed time for these basic tasks.
 */
fun noopNdjson(inputFile: Path, outputFile: Path) {
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
