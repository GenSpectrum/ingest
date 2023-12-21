package org.genspectrum.ingest.util

data class GeoLocation (
    val region: String,
    val country: String,
    val division: String,
    val location: String,
)

data class NullableGeoLocation (
    val region: String?,
    val country: String?,
    val division: String?,
    val location: String?,
)
