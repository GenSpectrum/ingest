package org.genspectrum.ingest.entry


fun MutableEntry.parseInteger(fieldName: String, ignoreErrors: Boolean = false) {
    this.metadata[fieldName] = try {
        this.metadata[fieldName]?.toString()?.toInt()
    } catch (e: NumberFormatException) {
        if (ignoreErrors) null
        else throw e
    }
}
