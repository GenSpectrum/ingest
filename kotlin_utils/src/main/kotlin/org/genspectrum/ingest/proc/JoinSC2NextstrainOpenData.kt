package org.genspectrum.ingest.proc

import org.genspectrum.ingest.AlignedGenome
import org.genspectrum.ingest.entry.*
import org.genspectrum.ingest.utils.SortedNdjsonFilesOuterJoiner
import org.genspectrum.ingest.utils.readFile
import org.genspectrum.ingest.utils.writeFile
import org.genspectrum.ingest.utils.writeNdjson
import java.nio.file.Path
import kotlin.io.path.Path

fun joinSC2NextstrainOpenData(
    sortedMetadataFile: Path,
    sortedNextcladeFile: Path,
    sortedSequencesFile: Path,
    sortedAlignedFile: Path,
    sortedTranslationFiles: List<Pair<String, Path>>,
    outputPath: Path
) {
    val translationNames = sortedTranslationFiles.map { it.first }
    val translationPaths = sortedTranslationFiles.map { it.second }
    val paths = mutableListOf(sortedMetadataFile, sortedNextcladeFile, sortedSequencesFile, sortedAlignedFile)
    paths.addAll(translationPaths)
    val inputStreams = paths.map { readFile(it) }

    val joiner = SortedNdjsonFilesOuterJoiner("strain", "seqName", inputStreams)
    val writer = writeNdjson<Any>(writeFile(outputPath))
    for ((_, values) in joiner) {
        val metadataEntry = (values[0]?.toMap() ?: continue).toMutableMap()
        val nextcladeEntry = values[1]?.toMap()
        if (nextcladeEntry != null) {
            metadataEntry += nextcladeEntry
        }
        val sequenceEntry = values[2]
        val alignedEntry = values[3]
        val translationSequences = values.subList(4, values.size).withIndex()
            .map { (index, entry) -> translationNames[index] to entry?.getString("sequence") }
        val nucleotideInsertionsText = nextcladeEntry?.get("insertions") as String?
        val nucleotideInsertions = mutableListOf<String>()
        if (!nucleotideInsertionsText.isNullOrBlank()) {
            nucleotideInsertions.addAll(nucleotideInsertionsText.split(","))
        }
        val aminoAcidInsertionsText = nextcladeEntry?.get("aaInsertions") as String?
        val aminoAcidInsertionsLists = sortedTranslationFiles.map { it.first to mutableListOf<String>() }
            .toTypedArray()
        val aminoAcidInsertions = mutableMapOf(*aminoAcidInsertionsLists)
        if (!aminoAcidInsertionsText.isNullOrBlank()) {
            aminoAcidInsertionsText
                .split(",")
                .forEach {
                    val (gene, insertion) = it.split(":", limit = 2)
                    aminoAcidInsertions[gene]!!.add(insertion)
                }
        }
        val joined = MutableEntry(
            metadataEntry["strain"] as String,
            metadataEntry,
            mutableMapOf("main" to sequenceEntry?.getString("sequence")),
            mutableMapOf("main" to alignedEntry?.getString("sequence")),
            translationSequences.toMap().toMutableMap(),
            mutableMapOf("main" to nucleotideInsertions),
            aminoAcidInsertions
        )
        clean(joined)
        if (joined.metadata["strain"] == null) {
            continue
        }
        writer.write(joined)
    }
    writer.close()
}

private val oldToNewMetadataNames = listOf(
    "gisaid_epi_isl" to "gisaidEpiIsl",
    "genbank_accession" to "genbankAccession",
    "genbank_accession_rev" to "genbankAccessionRev",
    "sra_accession" to "sraAccession",
    "region_exposure" to "regionExposure",
    "country_exposure" to "countryExposure",
    "division_exposure" to "divisionExposure",
    "Nextstrain_clade" to "nextstrainClade",
    "pango_lineage" to "pangoLineage",
    "GISAID_clade" to "gisaidClade",
    "originating_lab" to "originatingLab",
    "submitting_lab" to "submittingLab",
    "date_submitted" to "dateSubmitted",
    "date_updated" to "dateUpdated",
    "sampling_strategy" to "samplingStrategy",
    "clade_nextstrain" to "nextstrainClade",
    "clade_who" to "whoClade",
    "Nextclade_pango" to "nextcladePangoLineage",
    "immune_escape" to "immuneEscape",
    "ace2_binding" to "ace2Binding",
    "QC_overall_score" to "nextcladeQcOverallScore",
    "qc.missingData.score" to "nextcladeQcMissingDataScore",
    "qc.mixedSites.score" to "nextcladeQcMixedSites",
    "qc.privateMutations.score" to "nextcladeQcPrivateMutationsScore",
    "qc.snpClusters.score" to "nextcladeQcSnpClustersScore",
    "qc.frameShifts.score" to "nextcladeQcFrameShiftsScore",
    "qc.stopCodons.score" to "nextcladeQcStopCodonsScore",
    "coverage" to "nextcladeCoverage"
)

private val selectedMetadata = setOf(
    "strain",
    "genbankAccession",
    "genbankAccessionRev",
    "sraAccession",
    "gisaidEpiIsl",
    "database",
    "date",
    "dateSubmitted",
    "dateUpdated",
    "region",
    "country",
    "division",
    "location",
    "regionExposure",
    "countryExposure",
    "divisionExposure",
    "host",
    "age",
    "sex",
    "samplingStrategy",
    "pangoLineage",
    "nextcladePangoLineage",
    "nextstrainClade",
    "whoClade",
    "gisaidClade",
    "ace2Binding",
    "immuneEscape",
    "originatingLab",
    "submittingLab",
    "authors",
    "nextcladeQcOverallScore",
    "nextcladeQcMissingDataScore",
    "nextcladeQcMixedSites",
    "nextcladeQcPrivateMutationsScore",
    "nextcladeQcSnpClustersScore",
    "nextcladeQcFrameShiftsScore",
    "nextcladeQcStopCodonsScore",
    "nextcladeCoverage"
)

private val parseDateFields = listOf("date", "dateSubmitted", "dateUpdated")

private val parseIntegerFields = listOf("age")

private val parseFloatFields = listOf(
    "ace2binding",
    "immuneEscape",
    "nextcladeQcOverallScore",
    "nextcladeQcMissingDataScore",
    "nextcladeQcMixedSitesScore",
    "nextcladeQcPrivateMutationsScore",
    "nextcladeQcSnpClustersScore",
    "nextcladeQcFrameShiftsScore",
    "nextcladeQcStopCodonsScore",
    "nextcladeCoverage"
)

private val fillInMissingAlignedSequencesTemplate =
    AlignedGenome.loadFromFile(Path("reference-genome.sc2.json"))
        .replaceContentWithUnknown()

private fun clean(entry: MutableEntry) {
    entry.apply {
        renameMetadata(oldToNewMetadataNames)
        selectMetadata(selectedMetadata)
        mapToNull()
        parseDateFields.forEach { parseDate(it) }
        parseIntegerFields.forEach { parseInteger(it) }
        parseFloatFields.forEach { parseFloat(it) }
        fillInMissingAlignedSequences(fillInMissingAlignedSequencesTemplate)
    }
}
