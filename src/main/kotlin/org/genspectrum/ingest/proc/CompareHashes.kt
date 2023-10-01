package org.genspectrum.ingest.proc

fun compareHashes(oldHashes: List<HashEntry>, newHashes: List<HashEntry>): ComparisonResult {
    val oldHashesMap = oldHashes.associate { it.id to it.hash }
    val added = mutableListOf<String>()
    val changed = mutableListOf<String>()
    val deleted = oldHashes.map { it.id }.toMutableSet()
    val unchanged = mutableListOf<String>()
    for ((id, newHash) in newHashes) {
        val oldHash = oldHashesMap[id]
        if (oldHash == null) {
            added.add(id)
        } else {
            deleted.remove(id)
            if (oldHash == newHash) {
                unchanged.add(id)
            } else {
                changed.add(id)
            }
        }
    }
    return ComparisonResult(added, changed, deleted.toList(), unchanged)
}

data class HashEntry(
    val id: String,
    val hash: String,
)

data class ComparisonResult(
    val added: List<String>,
    val changed: List<String>,
    val deleted: List<String>,
    val unchanged: List<String>,
)
