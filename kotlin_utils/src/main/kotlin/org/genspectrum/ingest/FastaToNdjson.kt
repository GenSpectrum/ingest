package org.genspectrum.ingest

import com.alibaba.fastjson2.toJSONByteArray
import org.genspectrum.ingest.utils.FastaReader
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import java.nio.file.Path

class FastaToNdjson {

    fun run(idColumn: String, input: Path, output: Path) {
        FastaReader(readFile(input)).use { reader ->
            writeFile(output).use { outputStream ->
                reader.forEach {
                    outputStream.write(mapOf(idColumn to it.sampleName, "sequence" to it.sequence).toJSONByteArray())
                    outputStream.write("\n".toByteArray())
                }

            }
        }
    }

}
