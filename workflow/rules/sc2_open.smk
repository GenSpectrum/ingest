rule download_from_nextstrain:
    output:
        path="data/sc2_open/01_from_source/{file}"
    run:
        shell("wget https://data.nextstrain.org/files/ncov/open/{wildcards.file} -O {output.path}")

rule transform_tsv_to_ndjson:
    input:
        path="data/sc2_open/01_from_source/{prefix}.tsv.zst"
    output:
        path="data/sc2_open/02_ndjson/{prefix,(metadata|nextclade)}.ndjson.zst"
    log:
        out = "data/sc2_open/02_ndjson/log/{prefix}_stdout.log",
        err = "data/sc2_open/02_ndjson/log/{prefix}_stderr.log"
    run:
        shell("java -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "tsv-to-ndjson {input.path} {output.path} 2> {log.err} 1> {log.out}")

rule transform_fasta_to_ndjson:
    input:
        path="data/sc2_open/01_from_source/{prefix}.fasta.zst"
    output:
        path="data/sc2_open/02_ndjson/{prefix,(aligned|sequences|translation.*)}.ndjson.zst"
    log:
        out = "data/sc2_open/02_ndjson/log/{prefix}_stdout.log",
        err = "data/sc2_open/02_ndjson/log/{prefix}_stderr.log"
    run:
        shell("java -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "fasta-to-ndjson strain {input.path} {output.path} 2> {log.err} 1> {log.out}")

rule sort_ndjson:
    input:
        path="data/sc2_open/02_ndjson/{prefix}.ndjson.zst"
    output:
        path="data/sc2_open/03_sorted/{prefix}.sorted.ndjson.zst"
    log:
        out = "data/sc2_open/03_sorted/log/{prefix}_stdout.log",
        err = "data/sc2_open/03_sorted/log/{prefix}_stderr.log"
    run:
        sortBy = "strain"
        if wildcards.prefix == "nextclade":
            sortBy = "seqName"
        shell("mkdir -p data/sc2_open/03_sorted/workdir_{wildcards.prefix}")
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "sort-ndjson {sortBy} {input.path} {output.path} "
              "data/sc2_open/03_sorted/workdir_{wildcards.prefix} 2> {log.err} 1> {log.out}")
        shell("rm -r data/sc2_open/03_sorted/workdir_{wildcards.prefix}")

rule join_files:
    input:
        metadata = "data/sc2_open/03_sorted/metadata.sorted.ndjson.zst",
        nextclade = "data/sc2_open/03_sorted/nextclade.sorted.ndjson.zst",
        sequences = "data/sc2_open/03_sorted/sequences.sorted.ndjson.zst",
        aligned = "data/sc2_open/03_sorted/aligned.sorted.ndjson.zst",
        translation_E = "data/sc2_open/03_sorted/translation_E.sorted.ndjson.zst",
        translation_M = "data/sc2_open/03_sorted/translation_M.sorted.ndjson.zst",
        translation_N = "data/sc2_open/03_sorted/translation_N.sorted.ndjson.zst",
        translation_ORF1a = "data/sc2_open/03_sorted/translation_ORF1a.sorted.ndjson.zst",
        translation_ORF1b = "data/sc2_open/03_sorted/translation_ORF1b.sorted.ndjson.zst",
        translation_ORF3a = "data/sc2_open/03_sorted/translation_ORF3a.sorted.ndjson.zst",
        translation_ORF6 = "data/sc2_open/03_sorted/translation_ORF6.sorted.ndjson.zst",
        translation_ORF7a = "data/sc2_open/03_sorted/translation_ORF7a.sorted.ndjson.zst",
        translation_ORF7b = "data/sc2_open/03_sorted/translation_ORF7b.sorted.ndjson.zst",
        translation_ORF8 = "data/sc2_open/03_sorted/translation_ORF8.sorted.ndjson.zst",
        translation_ORF9b = "data/sc2_open/03_sorted/translation_ORF9b.sorted.ndjson.zst",
        translation_S = "data/sc2_open/03_sorted/translation_S.sorted.ndjson.zst"
    output:
        path="data/sc2_open/04_joined/raw.ndjson.zst"
    log:
        out = "data/sc2_open/04_joined/log/stdout.log",
        err = "data/sc2_open/04_joined/log/stderr.log"
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "join-sc2-nextstrain-open-data "
              "--sorted-metadata {input.metadata} "
              "--sorted-nextclade {input.nextclade} "
              "--sorted-sequences {input.sequences} "
              "--sorted-aligned {input.aligned} "
              "--sorted-translation-e {input.translation_E} "
              "--sorted-translation-m {input.translation_M} "
              "--sorted-translation-n {input.translation_N} "
              "--sorted-translation-orf1a {input.translation_ORF1a} "
              "--sorted-translation-orf1b {input.translation_ORF1b} "
              "--sorted-translation-orf3a {input.translation_ORF3a} "
              "--sorted-translation-orf6 {input.translation_ORF6} "
              "--sorted-translation-orf7a {input.translation_ORF7a} "
              "--sorted-translation-orf7b {input.translation_ORF7b} "
              "--sorted-translation-orf8 {input.translation_ORF8} "
              "--sorted-translation-orf9b {input.translation_ORF9b} "
              "--sorted-translation-s {input.translation_S} "
              "--output {output.path} 2> {log.err} 1> {log.out}"
        )

rule process:
    input:
        path="data/sc2_open/04_joined/raw.ndjson.zst"
    output:
        path="data/sc2_open/05_processed/processed.ndjson.zst"
    log:
        out = "data/sc2_open/05_processed/log/stdout.log",
        err = "data/sc2_open/05_processed/log/stderr.log"
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "process-ndjson "
              "--rename-metadata gisaid_epi_isl:gisaidEpiIsl "
              "--rename-metadata genbank_accession:genbankAccession "
              "--rename-metadata genbank_accession_rev:genbankAccessionRev "
              "--rename-metadata sra_accession:sraAccession "
              "--rename-metadata region_exposure:regionExposure "
              "--rename-metadata country_exposure:countryExposure "
              "--rename-metadata division_exposure:divisionExposure "
              "--rename-metadata Nextstrain_clade:nextstrainClade "
              "--rename-metadata pango_lineage:pangoLineage "
              "--rename-metadata GISAID_clade:gisaidClade "
              "--rename-metadata originating_lab:originatingLab "
              "--rename-metadata submitting_lab:submittingLab "
              "--rename-metadata date_submitted:dateSubmitted "
              "--rename-metadata date_updated:dateUpdated "
              "--rename-metadata sampling_strategy:samplingStrategy "
              "--rename-metadata clade_nextstrain:nextstrainClade "
              "--rename-metadata clade_who:whoClade "
              "--rename-metadata Nextclade_pango:nextcladePangoLineage "
              "--rename-metadata immune_escape:immuneEscape "
              "--rename-metadata ace2_binding:ace2Binding "
              "--rename-metadata QC_overall_score:nextcladeQcOverallScore "
              "--rename-metadata qc.missingData.score:nextcladeQcMissingDataScore "
              "--rename-metadata qc.mixedSites.score:nextcladeQcMixedSites "
              "--rename-metadata qc.privateMutations.score:nextcladeQcPrivateMutationsScore "
              "--rename-metadata qc.snpClusters.score:nextcladeQcSnpClustersScore "
              "--rename-metadata qc.frameShifts.score:nextcladeQcFrameShiftsScore "
              "--rename-metadata qc.stopCodons.score:nextcladeQcStopCodonsScore "
              "--rename-metadata coverage:nextcladeCoverage "
              "--select-metadata strain,genbankAccession,genbankAccessionRev,sraAccession,gisaidEpiIsl,database,date,dateSubmitted,dateUpdated,region,country,division,location,regionExposure,countryExposure,divisionExposure,host,age,sex,samplingStrategy,pangoLineage,nextcladePangoLineage,nextstrainClade,whoClade,gisaidClade,ace2Binding,immuneEscape,originatingLab,submittingLab,authors,nextcladeQcOverallScore,nextcladeQcMissingDataScore,nextcladeQcMixedSites,nextcladeQcPrivateMutationsScore,nextcladeQcSnpClustersScore,nextcladeQcFrameShiftsScore,nextcladeQcStopCodonsScore,nextcladeCoverage "
              "--map-to-null "
              "--parse-date date "
              "--parse-date dateSubmitted "
              "--parse-date dateUpdated "
              "--parse-integer age "
              "--parse-float ace2binding "
              "--parse-float immuneEscape "
              "--parse-float nextcladeQcOverallScore "
              "--parse-float nextcladeQcMissingDataScore "
              "--parse-float nextcladeQcMixedSitesScore "
              "--parse-float nextcladeQcPrivateMutationsScore "
              "--parse-float nextcladeQcSnpClustersScore "
              "--parse-float nextcladeQcFrameShiftsScore "
              "--parse-float nextcladeQcStopCodonsScore "
              "--parse-float nextcladeCoverage "
              "--fill-in-missing-aligned-sequences kotlin_utils/reference-genome.sc2.json "
              "{input.path} {output.path} 2> {log.err} 1> {log.out}"
        )
