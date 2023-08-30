package org.genspectrum.ingest.utils

import java.io.OutputStream

data class FastaEntry(val sampleName: String, val sequence: String) {
    fun writeToStream(outputStream: OutputStream) {
        outputStream.write(">".toByteArray())
        outputStream.write(sampleName.toByteArray())
        outputStream.write("\n".toByteArray())
        outputStream.write(sequence.toByteArray())
        outputStream.write("\n".toByteArray())
    }
}
