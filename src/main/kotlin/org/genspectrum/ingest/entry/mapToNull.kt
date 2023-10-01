package org.genspectrum.ingest.entry


/**
 * This function replaces given values with null. By default, it replaces "" and "?".
 */
fun MutableEntry.mapToNull(nullValues: Set<String> = setOf("", "?")) {
    for ((key, value) in this.metadata) {
        if (nullValues.contains(value)) {
            this.metadata[key] = null
        }
    }
}
