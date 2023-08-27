import csv
import shelve
import json
from Bio import SeqIO
import zstandard as zstd
import io
from tqdm import tqdm


def tsv_zstd_to_shelf(name, tsv_file, shelf_file, id_column):
    """
    This function stores the entries of a TSV file in a "shelve" file. It stores each row at
    `shelve_db[<id>###<name>]`

    :param name: The name under which the content of the file should be stored
    :param tsv_file: Path to the tsv.zst file
    :param shelf_file: Path to the "shelve" file
    :param id_column: Name of the ID column
    :return:
    """
    with shelve.open(shelf_file) as db:
        with open(tsv_file, 'rb') as compressed_in:
            decompressor = zstd.ZstdDecompressor()
            with decompressor.stream_reader(compressed_in) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                reader = csv.DictReader(text_stream, delimiter='\t')
                for record in tqdm(reader, total=9000000):
                    db["{}###{}".format(record[id_column], name)] = record


def fasta_zstd_to_shelf(name, fasta_file, shelf_file):
    """
    This function stores the entries of a FASTA file in a "shelve" file. It stores each row at
    `shelve_db[<id>###<name>]`

    :param name: The name under which the content of the file should be stored
    :param fasta_file: Path to the fasta.zst file
    :param shelf_file: Path to the "shelve" file
    :return:
    """
    with shelve.open(shelf_file) as db:
        with open(fasta_file, 'rb') as compressed_in:
            decompressor = zstd.ZstdDecompressor()
            with decompressor.stream_reader(compressed_in) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for record in tqdm(SeqIO.parse(text_stream, "fasta"), total=9000000):
                    db["{}###{}".format(record.id, name)] = str(record.seq)


def sort_tsv_and_transform_to_ndjson_and_return_id_list(tsv_file, output_file, id_column):
    # Read and sort file
    with open(tsv_file, 'rb') as compressed_in:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_in) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            rows = csv.DictReader(text_stream, delimiter='\t')
            sorted_rows = sorted(rows, key=lambda row: row[id_column])

    # Write
    with open(output_file, 'wb') as compressed_out:
        compressor = zstd.ZstdCompressor()
        with compressor.stream_writer(compressed_out) as writer:
            for row in sorted_rows:
                json_string = json.dumps(row)
                writer.write(json_string.encode("utf-8"))
                writer.write(b"\n")

    # Return sorted ID list
    return [row[id_column] for row in sorted_rows]


def get_field_from_ndjson(ndjson_file, column_name):
    result = []
    with open(ndjson_file, 'rb') as compressed_in:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_in) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in tqdm(text_stream):
                data = json.loads(line.strip())
                result.append(data[column_name])
    return result


def split_into_ranges(max_number_per_range, sorted_ids):
    ranges = []
    start = None
    i = 0
    for current_id in sorted_ids:
        if start is None:
            start = current_id
        i += 1
        if i >= max_number_per_range:
            ranges.append([start, current_id])
            start = None
            i = 0
    if start is not None:
        ranges.append([start, sorted_ids[-1]])
    return ranges


def find_range_index(item, ranges):
    for index, [start, end] in enumerate(ranges):
        if start <= item <= end:
            return index
    return None


def split_fasta_into_small_ndjson_files(fasta_file, ranges, output_file_prefix):
    output_streams = []
    writers = []
    compressor = zstd.ZstdCompressor()
    for index, _ in enumerate(ranges):
        compressed_out = open("{}.{}".format(output_file_prefix, index), 'wb')
        writer = compressor.stream_writer(compressed_out)
        output_streams.append(compressed_out)
        writers.append(writer)

    with open(fasta_file, 'rb') as compressed_in:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_in) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for record in tqdm(SeqIO.parse(text_stream, "fasta")):
                index = find_range_index(record.id, ranges)
                if index is None:
                    continue
                json_string = json.dumps({
                    "id": record.id,
                    "sequence": str(record.seq)
                })
                writer = writers[index]
                writer.write(json_string.encode("utf-8"))
                writer.write(b"\n")

    for writer in writers:
        writer.close()
    for output_stream in output_streams:
        output_stream.close()


# tsv_zstd_to_shelf('metadata', 'data/sc2_open/metadata.tsv.zst', 'metadata.shelf', 'strain')
# fasta_zstd_to_shelf('nucleotide_sequences_unaligned', 'data/sc2_open/sequences.fasta.zst', 'sequences.shelf')
# fasta_zstd_to_shelf('aa_sequences_E', 'data/sc2_open/translation_E.fasta.zst', 'translation_E.shelf')

# import shelve
# db = shelve.open("test.shelf")
# len(db)
# keys = list(db.keys())


def main():
    # For 8.6 million rows, this takes approx. 23 minutes
    # ids = sort_tsv_and_transform_to_ndjson_and_return_id_list(
    #     "data/sc2_open/metadata.tsv.zst", "data/sc2_open/metadata.sorted.ndjson.zst", "strain")

    ids = get_field_from_ndjson("data/sc2_open/metadata.sorted.ndjson.zst", "strain")
    ranges = split_into_ranges(300000, ids)
    print(ranges)

    # Approx. 40 minutes for sequences.fasta
    split_fasta_into_small_ndjson_files(
        'data/sc2_open/translation_E.fasta.zst', ranges, 'data/sc2_open/translation_E.fasta.zst')


main()
