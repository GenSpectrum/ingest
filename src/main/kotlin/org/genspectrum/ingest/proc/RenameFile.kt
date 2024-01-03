package org.genspectrum.ingest.proc

import java.nio.file.Path

fun renameFile(
    oldPath: Path,
    newPath: Path
) {
    oldPath.toFile().renameTo(newPath.toFile())
}
