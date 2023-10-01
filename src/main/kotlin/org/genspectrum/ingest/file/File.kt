package org.genspectrum.ingest.file

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
