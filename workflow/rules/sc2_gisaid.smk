import os

rule download_from_gisaid:
    output:
        path="data/sc2_gisaid/01_from_source/provision.ndjson.xz"
    params:
        user=os.environ["GISAID_USERNAME"],
        password=os.environ["GISAID_PASSWORD"]
    run:
        url = "https://www.epicov.org/epi3/3p/ethz/export/provision.json.xz"
        shell("curl -u {params.user}:{params.password} {url} > {output.path}")

rule basic_transform:
    input:
        path="data/sc2_gisaid/01_from_source/{file}.ndjson.xz"
    output:
        data="data/sc2_gisaid/02_basic_transformed/{file}.ndjson.zst",
        hashes="data/sc2_gisaid/02_basic_transformed/{file}.hashes.ndjson.zst"
    log:
        out="data/sc2_gisaid/02_basic_transformed/log/{file}.stdout.log",
        err="data/sc2_gisaid/02_basic_transformed/log/{file}.stderr.log"
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "transform-sc2-gisaid-basics {input.path} {output.data} {output.hashes} 2> {log.err} 1> {log.out}"
        )

rule compare_hashes:
    input:
        old="data/sc2_gisaid/02_basic_transformed/provision.01.hashes.ndjson.zst",
        new="data/sc2_gisaid/02_basic_transformed/provision.02.hashes.ndjson.zst"
    output:
        path="data/sc2_gisaid/03_comparison/comparison.json.zst"
    log:
        out="data/sc2_gisaid/03_comparison/log/stdout.log",
        err="data/sc2_gisaid/03_comparison/log/stderr.log"
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "compare-hashes gisaidEpiIsl {input.old} {input.new} {output.path} 2> {log.err} 1> {log.out}"
        )

rule extract_added_or_changed:
    input:
        data="data/sc2_gisaid/02_basic_transformed/provision.02.ndjson.zst",
        changes="data/sc2_gisaid/03_comparison/comparison.json.zst"
    output:
        unsorted="data/sc2_gisaid/04_added_or_changed/provision.ndjson.zst",
        sorted="data/sc2_gisaid/04_added_or_changed/provision.sorted.ndjson.zst"
    log:
        out="data/sc2_gisaid/04_added_or_changed/log/stdout.log",
        err="data/sc2_gisaid/04_added_or_changed/log/stderr.log"
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "extract-added-or-changed gisaidEpiIsl {input.changes} {input.data} {output.unsorted} "
              "2> {log.err} 1> {log.out}"
        )
        workdir="data/sc2_gisaid/04_added_or_changed/workdir"
        shell("mkdir -p {workdir}")
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "sort-ndjson id {output.unsorted} {output.sorted} {workdir} "
              "2>> {log.err} 1>> {log.out}"
        )
        shell("rm -r {workdir}")

rule run_nextclade:
    input:
        path="data/sc2_gisaid/04_added_or_changed/provision.sorted.ndjson.zst"
    output:
        fasta="data/sc2_gisaid/05_added_or_changed_nextclade/provision.sorted.fasta.zst",
        nextcladeTsv="data/sc2_gisaid/05_added_or_changed_nextclade/nextclade.sorted.tsv.zst",
        nextcladeAligned="data/sc2_gisaid/05_added_or_changed_nextclade/aligned.sorted.fasta.zst",
        nextcladeTranslations_E="data/sc2_gisaid/05_added_or_changed_nextclade/translation_E.sorted.fasta.zst",
        nextcladeTranslations_M="data/sc2_gisaid/05_added_or_changed_nextclade/translation_M.sorted.fasta.zst",
        nextcladeTranslations_N="data/sc2_gisaid/05_added_or_changed_nextclade/translation_N.sorted.fasta.zst",
        nextcladeTranslations_ORF1a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF1a.sorted.fasta.zst",
        nextcladeTranslations_ORF1b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF1b.sorted.fasta.zst",
        nextcladeTranslations_ORF3a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF3a.sorted.fasta.zst",
        nextcladeTranslations_ORF6="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF6.sorted.fasta.zst",
        nextcladeTranslations_ORF7a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF7a.sorted.fasta.zst",
        nextcladeTranslations_ORF7b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF7b.sorted.fasta.zst",
        nextcladeTranslations_ORF8="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF8.sorted.fasta.zst",
        nextcladeTranslations_ORF9b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF9b.sorted.fasta.zst",
        nextcladeTranslations_S="data/sc2_gisaid/05_added_or_changed_nextclade/translation_S.sorted.fasta.zst"
    log:
        fastaOut="data/sc2_gisaid/05_added_or_changed_nextclade/log/to-fasta.stdout.log",
        fastaErr="data/sc2_gisaid/05_added_or_changed_nextclade/log/to-fasta.stderr.log",
        nextcladeOut="data/sc2_gisaid/05_added_or_changed_nextclade/log/nextclade.stdout.log",
        nextcladeErr="data/sc2_gisaid/05_added_or_changed_nextclade/log/nextclade.stderr.log",
    run:
        shell("java -Xmx6g -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "unaligned-nucleotide-sequences-to-fasta "
              "gisaidEpiIsl main {input.path} {output.fasta} 2> {log.fastaErr} 1> {log.fastaOut}"
        )
        shell("external_tools/nextclade run -d sars-cov-2 -j 4 --in-order "
              "--output-tsv {output.nextcladeTsv} "
              "--output-fasta {output.nextcladeAligned} "
              "--output-translations data/sc2_gisaid/05_added_or_changed_nextclade/translation_{{gene}}.sorted.fasta.zst "
              "{output.fasta} 2> {log.nextcladeErr} 1> {log.nextcladeOut}"
        )

rule transform_tsv_to_ndjson2:
    input:
        path="data/sc2_gisaid/05_added_or_changed_nextclade/{prefix}.tsv.zst"
    output:
        path="data/sc2_gisaid/05_added_or_changed_nextclade/{prefix,(nextclade).*}.ndjson.zst"
    log:
        out="data/sc2_gisaid/05_added_or_changed_nextclade/log/{prefix}_transform_stdout.log",
        err="data/sc2_gisaid/05_added_or_changed_nextclade/log/{prefix}_transform_stderr.log"
    run:
        shell("java -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "tsv-to-ndjson {input.path} {output.path} 2> {log.err} 1> {log.out}")

rule transform_fasta_to_ndjson2:
    input:
        path="data/sc2_gisaid/05_added_or_changed_nextclade/{prefix}.fasta.zst"
    output:
        path="data/sc2_gisaid/05_added_or_changed_nextclade/{prefix,(aligned|translation).*}.ndjson.zst"
    log:
        out="data/sc2_gisaid/05_added_or_changed_nextclade/log/{prefix}_transform_stdout.log",
        err="data/sc2_gisaid/05_added_or_changed_nextclade/log/{prefix}_transform_stderr.log"
    run:
        shell("java -jar kotlin_utils/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar "
              "fasta-to-ndjson gisaidEpiIsl {input.path} {output.path} 2> {log.err} 1> {log.out}")

rule join_files2:
    input:
        provision="data/sc2_gisaid/04_added_or_changed/provision.sorted.ndjson.zst",
        nextcladeTsv="data/sc2_gisaid/05_added_or_changed_nextclade/nextclade.sorted.ndjson.zst",
        nextcladeAligned="data/sc2_gisaid/05_added_or_changed_nextclade/aligned.sorted.ndjson.zst",
        nextcladeTranslations_E="data/sc2_gisaid/05_added_or_changed_nextclade/translation_E.sorted.ndjson.zst",
        nextcladeTranslations_M="data/sc2_gisaid/05_added_or_changed_nextclade/translation_M.sorted.ndjson.zst",
        nextcladeTranslations_N="data/sc2_gisaid/05_added_or_changed_nextclade/translation_N.sorted.ndjson.zst",
        nextcladeTranslations_ORF1a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF1a.sorted.ndjson.zst",
        nextcladeTranslations_ORF1b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF1b.sorted.ndjson.zst",
        nextcladeTranslations_ORF3a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF3a.sorted.ndjson.zst",
        nextcladeTranslations_ORF6="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF6.sorted.ndjson.zst",
        nextcladeTranslations_ORF7a="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF7a.sorted.ndjson.zst",
        nextcladeTranslations_ORF7b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF7b.sorted.ndjson.zst",
        nextcladeTranslations_ORF8="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF8.sorted.ndjson.zst",
        nextcladeTranslations_ORF9b="data/sc2_gisaid/05_added_or_changed_nextclade/translation_ORF9b.sorted.ndjson.zst",
        nextcladeTranslations_S="data/sc2_gisaid/05_added_or_changed_nextclade/translation_S.sorted.ndjson.zst"
    output:
        path="data/sc2_gisaid/06_added_or_changed_joined/test.txt"
    run:
        shell("touch {output.path}")
