package org.genspectrum.ingest.proc

import org.genspectrum.ingest.MutableEntry

/**
 * This function sets "" and "?" to null.
 */
fun MutableEntry.mapToNull() {
    for ((key, value) in this.metadata) {
        if (value == "" || value == "?") {
            this.metadata[key] = null
        }
    }
}
