package org.genspectrum.ingest.util

import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.stream.Collectors

/**
 * This uses geolocation rules [1] maintained by Nextstrain to clean up the location data in GISAID. It is implemented
 * similarly to [2].
 *
 * [1] https://github.com/nextstrain/ncov-ingest/blob/master/source-data/gisaid_geoLocationRules.tsv
 * [2] https://github.com/nextstrain/ncov-ingest/blob/04ca33cbed1f96320035b9f7ebcc6abf4fa25a72/lib/utils/transformpipeline/transforms.py
 */
class GeoLocationMapper(geoLocationRules: List<String>) {
    private val rules: MutableMap<String, MutableMap<String, MutableMap<String, MutableMap<String, List<String>>>>> =
        mutableMapOf()

    /**
     * @param geoLocationRulesFile The path to gisaid_geoLocationRules.tsv
     */
    constructor(geoLocationRulesFile: Path) : this(
        Files.lines(geoLocationRulesFile).collect(Collectors.toList<String>())
    )

    /**
     * @param geoLocationRules The content of gisaid_geoLocationRules.tsv
     */
    init {
        for (rule in geoLocationRules) {
            val leftRight = rule.split("\t")
            if (leftRight.size != 2) {
                throw RuntimeException(
                    "Unexpected line in gisaid_geoLocationRules.tsv, split by tab failed. Found: $rule"
                )
            }
            val left = leftRight[0].lowercase().split("/")
            val right = leftRight[1].split("/")
            if (left.size != 4 || right.size != 4) {
                throw RuntimeException("Unexpected line in gisaid_geoLocationRules.tsv, split by / failed. Found: $rule");
            }
            rules.putIfAbsent(left[0], mutableMapOf())
            rules[left[0]]!!.putIfAbsent(left[1], mutableMapOf())
            rules[left[0]]!![left[1]]!!.putIfAbsent(left[2], mutableMapOf())
            rules[left[0]]!![left[1]]!![left[2]]!![left[3]] = listOf(right[0], right[1], right[2], right[3])
        }
    }

    private fun findApplicableRule(geoLocation: GeoLocation): List<String>? {
        return findApplicableRule(
            listOf(geoLocation.region, geoLocation.country, geoLocation.division, geoLocation.location),
            0,
            rules
        )
    }

    private fun findApplicableRule(
        geoLocation: List<String>,
        currentLevel: Int,
        currentLevelMap: Map<String, *>
    ): List<String>? {
        val fullMatchValue = currentLevelMap[geoLocation[currentLevel].lowercase()]
        val wildCastValue = currentLevelMap["*"]
        if (fullMatchValue == null && wildCastValue == null) {
            return null
        }
        if (currentLevel == 3) {
            return (fullMatchValue ?: wildCastValue) as List<String>
        }
        val fullMatchMap = fullMatchValue as Map<String, *>?
        val wildCastMap = wildCastValue as Map<String, *>?
        val nextLevelMap: Map<String, *> = if (fullMatchMap == null) {
            wildCastMap!!
        } else if (wildCastMap == null) {
            fullMatchMap
        } else {
            val _nextLevelMap: MutableMap<String, Any> = HashMap(fullMatchMap)
            wildCastMap.forEach { (key, value) -> _nextLevelMap.putIfAbsent(key, value!!) }
            _nextLevelMap
        }
        return findApplicableRule(geoLocation, currentLevel + 1, nextLevelMap)
    }

    fun resolve(geoLocation: NullableGeoLocation): NullableGeoLocation {
        val normalizedGeoLocation = GeoLocation(
            if (geoLocation.region != null) geoLocation.region.trim() else "",
            if (geoLocation.country != null) geoLocation.country.trim() else "",
            if (geoLocation.division != null) geoLocation.division.trim() else "",
            if (geoLocation.location != null) geoLocation.location.trim() else ""
        )
        val (region, country, division, location) = resolve(normalizedGeoLocation, 0)
        return NullableGeoLocation(
            if (region != "") region else null,
            if (country != "") country else null,
            if (division != "") division else null,
            if (location != "") location else null
        )
    }

    private fun resolve(geoLocation: GeoLocation, numberOfAppliedRules: Int): GeoLocation {
        if (numberOfAppliedRules > 1000) {
            throw RuntimeException(
                "More than 1000 geographic location rules applied on the same entry. " +
                        "There might be cyclicity in your rules. GeoLocation: " + geoLocation
            )
        }
        val rule = findApplicableRule(geoLocation)
        if (rule == null) {
            return geoLocation
        }
        val resolvedGeoLocation = GeoLocation(
            if (rule[0] != "*") rule[0] else geoLocation.region,
            if (rule[1] != "*") rule[1] else geoLocation.country,
            if (rule[2] != "*") rule[2] else geoLocation.division,
            if (rule[3] != "*") rule[3] else geoLocation.location,
        )
        return if (geoLocation == resolvedGeoLocation) {
            geoLocation
        } else resolve(resolvedGeoLocation, numberOfAppliedRules + 1)
    }
}
