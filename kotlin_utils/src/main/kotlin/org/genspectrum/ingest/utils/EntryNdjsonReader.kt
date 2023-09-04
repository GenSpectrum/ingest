package org.genspectrum.ingest.utils

import com.alibaba.fastjson2.parseObject
import org.genspectrum.ingest.MutableEntry
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

class EntryNdjsonReader(inputStream: InputStream) : Iterator<MutableEntry>, Iterable<MutableEntry>, AutoCloseable {

    private val reader: BufferedReader
    private var nextEntry: MutableEntry? = null

    init {
        reader = BufferedReader(InputStreamReader(inputStream))
        read()
    }

    override fun hasNext(): Boolean {
        return nextEntry != null
    }

    override fun next(): MutableEntry {
        val entry = nextEntry ?: throw NoSuchElementException("No element available")
        read()
        return entry
    }

    private fun read() {
        nextEntry = reader.readLine()?.parseObject<MutableEntry>()
    }

    override fun close() {
        reader.close()
    }

    override fun iterator(): Iterator<MutableEntry> {
        return this
    }

}
