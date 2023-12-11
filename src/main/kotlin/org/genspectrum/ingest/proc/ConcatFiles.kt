package org.genspectrum.ingest.proc

import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path

fun concatFiles(
    inputFiles: Array<Path>,
    outputFile: Path,
) {
    val out = FileOutputStream(outputFile.toFile()).channel
    for (inputFile in inputFiles) {
        val input = FileInputStream(inputFile.toFile()).channel
        out.transferFrom(input, out.size(), input.size())
    }
}
