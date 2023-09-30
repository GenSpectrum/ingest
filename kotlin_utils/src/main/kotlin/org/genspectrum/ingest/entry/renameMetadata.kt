package org.genspectrum.ingest.entry

fun MutableEntry.renameMetadata(oldToNewNames: Iterable<Pair<String, String>>) {
    for ((old, new) in oldToNewNames) {
        this.metadata[new] = this.metadata[old]
        this.metadata.remove(old)
    }
}
