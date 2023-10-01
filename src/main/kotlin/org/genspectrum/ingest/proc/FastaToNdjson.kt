package org.genspectrum.ingest.proc

import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.util.FastaReader
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.writeFile
import org.genspectrum.ingest.util.writeNdjson
import java.nio.file.Path

fun fastaToNdjson(
    idColumn: String,
    input: File,
    outputDirectory: Path = input.directory,
    outputName: String = input.name,
    outputCompression: Compression = input.compression
): File {
    require(input.type == FileType.FASTA)
    val outputFile = File(outputName, outputDirectory, input.sorted, FileType.NDJSON, outputCompression)

    FastaReader(readFile(input.path)).use { reader ->
        val writer = writeNdjson<Any>(writeFile(outputFile.path))
        reader.forEach {
            writer.write(mapOf(idColumn to it.sampleName, "sequence" to it.sequence))
        }
        writer.close()
    }

    return outputFile
}
