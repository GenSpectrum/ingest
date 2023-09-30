package org.genspectrum.ingest

import com.alibaba.fastjson2.JSONObject
import org.genspectrum.ingest.proc.mapToNull
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.readNdjson
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path

class TransformSC2GisaidBasics {

    fun run(inputFile: Path, outputFile: Path, hashOutputFile: Path) {
        val reader = readNdjson<JSONObject>(readFile(inputFile))
        val hashFileWriter = writeNdjson<Any>(writeFile(hashOutputFile))
        val writer = writeNdjson<MutableEntry>(
            writeFile(outputFile),
            fun (entry, hash) {
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
            writer.write(ourEntry)
        }
        writer.close()
        hashFileWriter.close()
    }

}
