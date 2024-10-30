package org.genspectrum.ingest.proc

import com.alibaba.fastjson2.to
import org.genspectrum.ingest.AlignedGenome
import org.genspectrum.ingest.entry.*
import org.genspectrum.ingest.file.AllPangoLineagesFile
import org.genspectrum.ingest.file.Compression
import org.genspectrum.ingest.file.File
import org.genspectrum.ingest.file.FileType
import org.genspectrum.ingest.util.SortedNdjsonFilesOuterJoiner
import org.genspectrum.ingest.util.readFile
import org.genspectrum.ingest.util.writeFile
import org.genspectrum.ingest.util.writeNdjson
import java.nio.file.Path
import kotlin.io.path.Path

fun joinSC2GisaidData(
    provisionFile: File,
    nextcladeFile: File,
    alignedFile: File,
    translationFiles: List<Pair<String, File>>,
    outputDirectory: Path,
    outputName: String,
    nextcladeDatasetVersion: String,
): Pair<File, AllPangoLineagesFile> {
    val allFiles = listOf(provisionFile, nextcladeFile, alignedFile) + translationFiles.map { it.second }
    require(allFiles.all { it.sorted && it.type == FileType.NDJSON })
    val outputFile = File(outputName, outputDirectory, true, FileType.NDJSON, Compression.ZSTD)

    val translationNames = translationFiles.map { it.first }
    val inputStreams = allFiles.map { readFile(it.path) }

    val allPangoLineages = HashSet<String>()

    val joiner = SortedNdjsonFilesOuterJoiner("id", "seqName", inputStreams)
    val writer = writeNdjson<Any>(writeFile(outputFile.path))
    for ((_, values) in joiner) {
        val provisionEntry = values[0]?.to<MutableEntry>() ?: continue
        val nextcladeEntry = values[1]?.toMap()
        if (nextcladeEntry != null) {
            provisionEntry.metadata += nextcladeEntry
        }

        val alignedEntry = values[2]
        val translationSequences = values.subList(3, values.size).withIndex()
            .map { (index, entry) -> translationNames[index] to entry?.getString("sequence") }
        val nucleotideInsertionsText = nextcladeEntry?.get("insertions") as String?
        val nucleotideInsertions = mutableListOf<String>()
        if (!nucleotideInsertionsText.isNullOrBlank()) {
            nucleotideInsertions.addAll(nucleotideInsertionsText.split(","))
        }
        val aminoAcidInsertionsText = nextcladeEntry?.get("aaInsertions") as String?
        val aminoAcidInsertionsLists = translationFiles.map { it.first to mutableListOf<String>() }
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

        provisionEntry.alignedNucleotideSequences = mutableMapOf("main" to alignedEntry?.getString("sequence"))
        provisionEntry.alignedAminoAcidSequences = translationSequences.toMap().toMutableMap()
        provisionEntry.nucleotideInsertions = mutableMapOf("main" to nucleotideInsertions)
        provisionEntry.aminoAcidInsertions = aminoAcidInsertions
        clean(provisionEntry)
        provisionEntry.metadata["nextcladeDatasetVersion"] = nextcladeDatasetVersion

        for (pangoLineageField in pangoLineageNames) {
            val pangoLineage = provisionEntry.metadata[pangoLineageField]
            if (pangoLineage is String) {
                allPangoLineages.add(pangoLineage)
            }
        }

        writer.write(provisionEntry)
    }
    writer.close()

    val allPangoLineagesFile = AllPangoLineagesFile(directory = outputDirectory)
    allPangoLineagesFile.write(allPangoLineages)

    return outputFile to allPangoLineagesFile
}

private const val pangoLineage = "pangoLineage"
private const val nextcladePangoLineage = "nextcladePangoLineage"

private val pangoLineageNames = listOf(pangoLineage, nextcladePangoLineage)

private val oldToNewMetadataNames = listOf(
    "clade_nextstrain" to "nextstrainClade",
    "clade_who" to "whoClade",
    "Nextclade_pango" to nextcladePangoLineage,
    "qc.overallScore" to "nextcladeQcOverallScore",
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
    "hospitalized",
    "died",
    "fullyVaccinated",
    "age",
    "sex",
    "samplingStrategy",
    pangoLineage,
    nextcladePangoLineage,
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
    "ace2Binding",
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
        maskSC2LeadingAndTrailingDeletions()
        renameMetadata(oldToNewMetadataNames)
        selectMetadata(selectedMetadata)
        mapToNull()
        parseDateFields.forEach { parseDate(it) }
        parseIntegerFields.forEach { parseInteger(it, true) }
        parseFloatFields.forEach { parseFloat(it) }
        fillInMissingAlignedSequences(fillInMissingAlignedSequencesTemplate)

        metadata["genbankAccession"] = null
        metadata["sraAccession"] = null
        metadata["regionExposure"] = null
        metadata["countryExposure"] = null
        metadata["divisionExposure"] = null
        metadata["gisaidClade"] = null
        metadata["ace2Binding"] = null
        metadata["immuneEscape"] = null
        metadata["originatingLab"] = null
        metadata["submittingLab"] = null
        metadata["authors"] = null
        metadata["dateUpdated"] = null
        metadata["died"] = null
        metadata["fullyVaccinated"] = null
        metadata["hospitalized"] = null
    }
}
