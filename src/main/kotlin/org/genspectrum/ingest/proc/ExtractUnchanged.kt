package org.genspectrum.ingest.proc

import com.alibaba.fastjson2.parseObject
import org.genspectrum.ingest.entry.MutableEntry
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.readNdjson
import org.genspectrum.ingest.util.writeFile
import org.genspectrum.ingest.util.writeNdjson
import java.nio.file.Path

fun extractUnchanged(
    idColumn: String,
    changeComparisonFile: Path,
    inputFile: File,
    outputDirectory: Path
): File {
    require(inputFile.type == FileType.NDJSON)
    val outputFile = File(inputFile.name, outputDirectory, inputFile.sorted, FileType.NDJSON, Compression.ZSTD)

    val changeComparison = readFile(changeComparisonFile).bufferedReader().readText()
        .parseObject<ComparisonResult>()
    val unchanged = changeComparison.unchanged.toSet()

    val reader = readNdjson<MutableEntry>(readFile(inputFile.path))
    val writer = writeNdjson<MutableEntry>(writeFile(outputFile.path))
    for (entry in reader) {
        if (unchanged.contains(entry.metadata[idColumn])) {
            writer.write(entry)
        }
    }
    writer.close()

    return outputFile
}
