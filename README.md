# GenSpectrum ingest

## Run with Docker

See help page:

```bash
docker run --rm ghcr.io/genspectrum/ingest:main -h
```

Preprocess GISAID data:

```bash
docker run --rm \
  -v <local data directory>:/data \
  ghcr.io/genspectrum/ingest:main \
  ingest-sc2-gisaid \
  /data \
  <data version of previous output> \
  <GISAID endpoint URL> \
  <GISAID username> \
  <GISAID password> \
  /app/gisaid_geoLocationRules.tsv
```


## Internal data format

The program uses `ndjson.zst` as the default format. One JSON entry could look as follows:

```json
{
    "id": "MW12345",
    "metadata": {
        "genbankAccession": "MW12345",
        "sequencingDate": "2022-08-15",
        "country": "Schweiz",
        "pangoLineage": "XBB.1.5"
    },
    "unalignedNucleotideSequences": {
        "main": "AATTCC..."
    },
    "alignedNucleotideSequences": {
        "main": "NNNNNAATTCC..."
    },
    "nucleotideInsertions": {
        // ???
    },
    "alignedAminoAcidSequences": {
        "S": "XXMSR...",
        "ORF1a": "...",
        // ...
    },
    "aminoAcidInsertions": {
        // ???
    }
}
```
