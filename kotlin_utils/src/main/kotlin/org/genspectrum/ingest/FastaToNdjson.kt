package org.genspectrum.ingest

import org.genspectrum.ingest.utils.FastaReader
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class FastaToNdjson {

    fun run(idColumn: String, input: Path, output: Path) {
        FastaReader(readFile(input)).use { reader ->
            val writer = writeNdjson<Any>(writeFile(output))
            reader.forEach {
                writer.write(mapOf(idColumn to it.sampleName, "sequence" to it.sequence))
            }
            writer.close()
        }
    }

}
