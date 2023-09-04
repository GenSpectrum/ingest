package org.genspectrum.ingest.utils

import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import java.io.*
import java.nio.file.Path

fun readFile(file: Path): InputStream {
    return ZstdInputStream(FileInputStream(file.toFile()))
}

fun writeFile(file: Path): OutputStream {
    val fileOut = FileOutputStream(file.toFile())
    val zstdOut = ZstdOutputStream(fileOut)
    return BufferedOutputStream(zstdOut)
}
