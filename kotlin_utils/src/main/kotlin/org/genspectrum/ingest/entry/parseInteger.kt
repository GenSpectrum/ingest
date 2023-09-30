package org.genspectrum.ingest.entry


fun MutableEntry.parseInteger(fieldName: String) {
    this.metadata[fieldName] = this.metadata[fieldName]?.toString()?.toInt()
}
