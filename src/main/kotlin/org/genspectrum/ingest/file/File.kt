package org.genspectrum.ingest.file

import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.writeFile
import java.nio.file.Path

data class File(
    val name: String,
    val directory: Path,
    val sorted: Boolean,
    val type: FileType,
    val compression: Compression
) {
    val filename: String
        get() {
            var n = name
            if (sorted) {
                n += ".sorted"
            }
            n += "." + type.toString().lowercase()
            n += when (compression) {
                Compression.NONE -> ""
                Compression.ZSTD -> ".zst"
                Compression.XZ -> ".xz"
            }
            return n
        }

    val path: Path = directory.resolve(filename)
}

enum class FileType {
    NDJSON,
    FASTA,
    TSV
}

enum class Compression {
    NONE,
    ZSTD,
    XZ
}

class AllPangoLineagesFile(
    dataVersion: String? = null,
    directory: Path
) {
    val file = File(
        name = when (dataVersion) {
            null -> "allPangoLineages"
            else -> "allPangoLineages.$dataVersion"
        },
        directory = directory,
        sorted = false,
        type = FileType.TSV,
        compression = Compression.NONE
    )

    val path
        get() = file.path

    fun read(): Set<String> {
        return readFile(file.path)
            .bufferedReader()
            .use { it.readText() }
            .lines()
            .toSet()
    }

    fun write(allPangoLineages: Collection<String>) {
        writeFile(file.path).bufferedWriter().use {
            for (pangoLineage in allPangoLineages) {
                it.write(pangoLineage)
                it.newLine()
            }
        }
    }
}
