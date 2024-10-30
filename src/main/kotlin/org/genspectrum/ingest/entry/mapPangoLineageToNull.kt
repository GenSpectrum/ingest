package org.genspectrum.ingest.entry


/**
 * This function cleans "non-pango lineages" from pango lineages columns
 */
fun MutableEntry.mapPangoLineageToNull(
    keys: Collection<String>,
    nullValues: Set<String> = setOf("Unassigned", "unclassifiable")
) {
    for (key in keys) {
        if (nullValues.contains(this.metadata[key])) {
            this.metadata[key] = null
        }
    }
}
