package org.genspectrum.ingest.entry

import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.regex.Pattern


/**
 * If fieldName = "sequencingDate", createYearMonthDateFields will add the fields "sequencingDateYear" (int),
 * "sequencingDateMonth" (int), "sequencingDateDay" (int). If createOriginalValueField will add the field
 * "sequencingDateOriginalValue" (string).
 *
 * These additional fields are useful to support partial dates. Partial date strings could look like "2022", "2022-05",
 * "2022-XX-XX", "2022-05-XX".
 */
fun MutableEntry.parseDate(
    fieldName: String,
    createYearMonthDateFields: Boolean = true,
    createOriginalValueField: Boolean = true
) {
    if (createYearMonthDateFields) {
        this.metadata["${fieldName}Year"] = null
        this.metadata["${fieldName}Month"] = null
        this.metadata["${fieldName}Day"] = null
    }
    if (createOriginalValueField) {
        this.metadata["${fieldName}OriginalValue"] = null
    }

    var value = this.metadata[fieldName]?.toString() ?: return
    if (createOriginalValueField) {
        this.metadata["${fieldName}OriginalValue"] = value
    }

    // Parse to LocalDate -> will only work if we have a complete date
    try {
        val localDate = LocalDate.parse(value)
        if (createYearMonthDateFields) {
            this.metadata["${fieldName}Year"] = localDate.year
            this.metadata["${fieldName}Month"] = localDate.monthValue
            this.metadata["${fieldName}Day"] = localDate.dayOfMonth
        }
        return
    } catch (ignored: DateTimeParseException) {
    }

    // Parse date parts if LocalDate was not able to parse the date
    value = value.replace("X", "0")
    val pattern: Pattern = Pattern.compile("(\\d{4})(-\\d{2})?(-\\d{2})?")
    val matcher = pattern.matcher(value)
    if (matcher.matches()) {
        val g1 = if (matcher.group(1) != null) matcher.group(1).toInt() else null
        val g2 = if (matcher.group(2) != null) matcher.group(2).substring(1).toInt() else null
        val g3 = if (matcher.group(3) != null) matcher.group(3).substring(1).toInt() else null
        if (g1 != null && g1 > 0) {
            this.metadata["${fieldName}Year"] = g1
        }
        if (g2 != null && g2 > 0 && g2 <= 12) {
            this.metadata["${fieldName}Month"] = g2
        }
        if (g3 != null && g3 > 0 && g3 <= 31) {
            this.metadata["${fieldName}Day"] = g3
        }
    }
    this.metadata[fieldName] = null
}
