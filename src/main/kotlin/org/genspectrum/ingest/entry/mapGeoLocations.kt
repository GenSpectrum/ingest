package org.genspectrum.ingest.entry

import org.genspectrum.ingest.util.GeoLocationMapper
import org.genspectrum.ingest.util.NullableGeoLocation

fun MutableEntry.mapGeoLocations(
    geoLocationMapper: GeoLocationMapper,
    regionFieldName: String = "region",
    countryFieldName: String = "country",
    divisionFieldName: String = "division",
    locationFieldName: String = "location",
) {
    val geoLocation = NullableGeoLocation(
        this.metadata[regionFieldName] as String?,
        this.metadata[countryFieldName] as String?,
        this.metadata[divisionFieldName] as String?,
        this.metadata[locationFieldName] as String?,
    )
    val resolved = geoLocationMapper.resolve(geoLocation)
    this.metadata[regionFieldName] = resolved.country
    this.metadata[countryFieldName] = resolved.country
    this.metadata[divisionFieldName] = resolved.country
    this.metadata[locationFieldName] = resolved.country
}
