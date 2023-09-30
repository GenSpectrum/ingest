package org.genspectrum.ingest.entry


fun MutableEntry.parseFloat(fieldName: String) {
    this.metadata[fieldName] = this.metadata[fieldName]?.toString()?.toDouble()
}
