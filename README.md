# GenSpectrum ingest

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
