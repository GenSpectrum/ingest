rule download_from_nextstrain:
    output:
        metadata="data/sc2_open/metadata.tsv.zst",
        sequences="data/sc2_open/sequences.fasta.zst",
        aligned="data/sc2_open/aligned.fasta.zst",
        translation_E="data/sc2_open/translation_E.fasta.zst"
    run:
        for path in output:
            file = path.split("/")[-1]
            shell("wget https://data.nextstrain.org/files/ncov/open/{file} -O {path}")



rule prepare_raw_input:
    input:
        metadata = "data/sc2_open/metadata.tsv.zst",
        sequences = "data/sc2_open/sequences.fasta.zst",
        aligned = "data/sc2_open/aligned.fasta.zst",
        translation_E = "data/sc2_open/translation_E.fasta.zst"
    output:
        "data/sc2_open/raw.ndjson.zst"

