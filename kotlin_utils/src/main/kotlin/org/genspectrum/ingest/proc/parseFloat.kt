package org.genspectrum.ingest.proc

import org.genspectrum.ingest.MutableEntry


fun MutableEntry.parseFloat(fieldName: String) {
    this.metadata[fieldName] = this.metadata[fieldName]?.toString()?.toDouble()
}
