package org.genspectrum.ingest.entry

fun MutableEntry.selectMetadata(fields: Set<String>) {
    for (key in this.metadata.keys.toList()) {
        if (!fields.contains(key)) {
            this.metadata.remove(key)
        }
    }
}
