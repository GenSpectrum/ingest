package org.genspectrum.ingest.proc

import org.genspectrum.ingest.entry.MutableEntry
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.util.FastaEntry
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.readNdjson
import org.genspectrum.ingest.util.writeFile
import java.nio.file.Path

fun unalignedNucleotideSequencesToFasta(
    idColumn: String,
    sequenceName: String,
    inputFile: File,
    outputDirectory: Path,
    outputName: String
): File {
    require(inputFile.type == FileType.NDJSON)
    val outputFile = File(outputName, outputDirectory, inputFile.sorted, FileType.FASTA, Compression.ZSTD)

    val reader = readNdjson<MutableEntry>(readFile(inputFile.path))
    writeFile(outputFile.path).use { outputStream ->
        for (entry in reader) {
            val fasta = FastaEntry(
                entry.metadata[idColumn] as String,
                entry.unalignedNucleotideSequences[sequenceName]!!
            )
            fasta.writeToStream(outputStream)
        }
    }

    return outputFile
}
