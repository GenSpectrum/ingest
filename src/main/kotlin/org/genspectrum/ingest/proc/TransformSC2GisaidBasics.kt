package org.genspectrum.ingest.proc

import com.alibaba.fastjson2.JSONObject
import org.genspectrum.ingest.entry.MutableEntry
import org.genspectrum.ingest.entry.mapGeoLocations
import org.genspectrum.ingest.entry.mapToNull
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.util.*
import java.nio.file.Path

fun transformSC2GisaidBasics(
    inputFile: File,
    outputDirectory: Path,
    geoLocationRulesFile: Path
): TransformSC2GisaidBasicsResult {
    require(inputFile.type == FileType.NDJSON)
    val outputFile = File(inputFile.name, outputDirectory, inputFile.sorted, FileType.NDJSON, Compression.ZSTD)
    val hashOutputFile = outputFile.copy(name = inputFile.name + ".hashes")
    val geoLocationMapper = GeoLocationMapper(geoLocationRulesFile)

    val reader = readNdjson<JSONObject>(readFile(inputFile.path))
    val hashFileWriter = writeNdjson<Any>(writeFile(hashOutputFile.path))
    val writer = writeNdjson<MutableEntry>(
        writeFile(outputFile.path),
        fun(entry, hash) {
            val hashEntry = mapOf(
                "gisaidEpiIsl" to entry.metadata["gisaidEpiIsl"],
                "md5" to hash
            )
            hashFileWriter.write(hashEntry)
        }
    )
    for (gisaidEntry in reader) {
        var region: String? = null
        var country: String? = null
        var divison: String? = null
        var location: String? = null
        val gisaidLocationString = gisaidEntry.getString("covv_location")
        if (gisaidLocationString.isNotBlank()) {
            val locationComponents = gisaidLocationString.split("/").map { it.trim() }
            region = locationComponents.getOrNull(0)
            country = locationComponents.getOrNull(1)
            divison = locationComponents.getOrNull(2)
            location = locationComponents.getOrNull(3)
        }
        val ourEntry = MutableEntry(
            gisaidEntry.getString("covv_accession_id"),
            mutableMapOf(
                "gisaidEpiIsl" to gisaidEntry.getString("covv_accession_id"),
                "strain" to gisaidEntry.getString("covv_virus_name"),
                "date" to gisaidEntry.getString("covv_collection_date"),
                "dateSubmitted" to gisaidEntry.getString("covv_subm_date"),
                "host" to gisaidEntry.getString("covv_host"),
                "age" to gisaidEntry.getString("covv_patient_age"),
                "sex" to gisaidEntry.getString("covv_gender"),
                "samplingStrategy" to gisaidEntry.getString("covv_sampling_strategy"),
                "pangoLineage" to gisaidEntry.getString("covv_lineage"),
                "region" to region,
                "country" to country,
                "division" to divison,
                "location" to location
            ),
            mutableMapOf(
                "main" to gisaidEntry.getString("sequence")
            ),
            mutableMapOf(),
            mutableMapOf(),
            mutableMapOf(),
            mutableMapOf()
        )
        ourEntry.mapToNull(setOf("", "unknown"))
        ourEntry.mapGeoLocations(geoLocationMapper)
        writer.write(ourEntry)
    }
    writer.close()
    hashFileWriter.close()

    return TransformSC2GisaidBasicsResult(outputFile, hashOutputFile)
}

data class TransformSC2GisaidBasicsResult (
    val dataFile: File,
    val hashesFile: File
)
