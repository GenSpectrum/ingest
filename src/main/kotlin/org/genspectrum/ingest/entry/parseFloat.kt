package org.genspectrum.ingest.entry


fun MutableEntry.parseFloat(fieldName: String, ignoreErrors: Boolean = false) {
    this.metadata[fieldName] = try {
        this.metadata[fieldName]?.toString()?.toDouble()
    } catch (e: NumberFormatException) {
        if (ignoreErrors) null
        else throw e
    }
}
