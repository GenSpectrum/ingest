package org.genspectrum.ingest.proc

import org.genspectrum.ingest.MutableEntry


fun MutableEntry.parseInteger(fieldName: String) {
    this.metadata[fieldName] = this.metadata[fieldName]?.toString()?.toInt()
}
