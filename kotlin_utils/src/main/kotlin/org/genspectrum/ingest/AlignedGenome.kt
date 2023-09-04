package org.genspectrum.ingest

import com.alibaba.fastjson2.parseObject
import java.nio.file.Path

data class AlignedGenome (
    val nucleotideSequences: Map<String, String>,
    val aminoAcidSequences: Map<String, String>,
)

fun loadFromFile(file: Path): AlignedGenome {
    return file.toFile().readText().parseObject<AlignedGenome>()
}

fun AlignedGenome.replaceContentWithUnknown(genome: AlignedGenome): AlignedGenome {
    return AlignedGenome(
        this.nucleotideSequences.map { (name, sequence) -> name to "N".repeat(sequence.length) }.toMap(),
        this.nucleotideSequences.map { (name, sequence) -> name to "X".repeat(sequence.length) }.toMap()
    )
}
