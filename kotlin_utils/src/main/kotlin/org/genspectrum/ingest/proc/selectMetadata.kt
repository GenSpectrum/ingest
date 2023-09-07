package org.genspectrum.ingest.proc

import org.genspectrum.ingest.MutableEntry

fun MutableEntry.selectMetadata(fields: Set<String>) {
    for (key in this.metadata.keys.toList()) {
        if (!fields.contains(key)) {
            this.metadata.remove(key)
        }
    }
}
