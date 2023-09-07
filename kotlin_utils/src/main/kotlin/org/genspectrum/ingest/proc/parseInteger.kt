package org.genspectrum.ingest.proc

import org.genspectrum.ingest.MutableEntry
import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.regex.Pattern


fun MutableEntry.parseInteger(fieldName: String) {
    this.metadata[fieldName] = this.metadata[fieldName]?.toString()?.toInt()
}
