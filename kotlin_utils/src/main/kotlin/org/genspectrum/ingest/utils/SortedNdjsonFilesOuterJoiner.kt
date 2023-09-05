package org.genspectrum.ingest.utils

import com.alibaba.fastjson2.JSONObject
import java.io.InputStream

class SortedNdjsonFilesOuterJoiner(private val joinColumn: String, inputStreams: List<InputStream>) :
    Iterator<Pair<String, List<JSONObject?>>>, Iterable<Pair<String, List<JSONObject?>>> {

    private val readers: List<Iterator<JSONObject>>
    private val currentEntries: MutableList<KeyValue?>
    private var nextEntry: Pair<String, List<JSONObject?>>? = null

    init {
        readers = inputStreams.map { readNdjson<JSONObject>(it).iterator() }
        currentEntries = readers.map { getNextFromFile(it) }.toMutableList()
        read()
    }

    private fun getNextFromFile(reader: Iterator<JSONObject>): KeyValue? {
        return try {
            val entry = reader.next()
            val joinValue = entry.getString(joinColumn)
            joinValue to entry
        } catch (e: NoSuchElementException) {
            null
        }
    }

    override fun hasNext(): Boolean {
        return nextEntry != null
    }

    override fun next(): Pair<String, List<JSONObject?>> {
        val entry = nextEntry ?: throw NoSuchElementException("No element available")
        read()
        return entry
    }

    private fun read() {
        val minKey = currentEntries.filterNotNull().minOfOrNull { it.first }
        if (minKey == null) {
            nextEntry = null
            return
        }

        nextEntry = minKey to currentEntries.map { if (it?.first == minKey) it.second else null }

        for (i in 0..<currentEntries.size) {
            if (currentEntries[i]?.first == minKey) {
                currentEntries[i] = getNextFromFile(readers[i])
            }
        }
    }

    override fun iterator(): Iterator<Pair<String, List<JSONObject?>>> {
        return this
    }
}

private typealias KeyValue = Pair<String, JSONObject>
