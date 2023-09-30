package org.genspectrum.ingest

import org.genspectrum.ingest.utils.FastaEntry
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import java.nio.file.Path

class UnalignedNucleotideSequencesToFasta {

    fun run(idColumn: String, sequenceName: String, inputFile: Path, outputFile: Path) {
        val reader = readNdjson<MutableEntry>(readFile(inputFile))
        writeFile(outputFile).use { outputStream ->
            for (entry in reader) {
                val fasta = FastaEntry(
                    entry.metadata[idColumn] as String,
                    entry.unalignedNucleotideSequences[sequenceName]!!
                )
                fasta.writeToStream(outputStream)
            }
        }
    }

}
