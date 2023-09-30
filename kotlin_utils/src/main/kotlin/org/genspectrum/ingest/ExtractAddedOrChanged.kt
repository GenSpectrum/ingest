package org.genspectrum.ingest

import com.alibaba.fastjson2.parseObject
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class ExtractAddedOrChanged {

    fun run(idColumn: String, changeComparisonFile: Path, inputFile: Path, outputFile: Path) {
        val changeComparison = readFile(changeComparisonFile).bufferedReader().readText()
            .parseObject<ComparisonResult>()
        val addedOrChanged = (changeComparison.added + changeComparison.changed).toSet()

        val reader = readNdjson<MutableEntry>(readFile(inputFile))
        val writer = writeNdjson<MutableEntry>(writeFile(outputFile))
        for (entry in reader) {
            if (addedOrChanged.contains(entry.metadata[idColumn])) {
                writer.write(entry)
            }
        }
        writer.close()
    }

}
