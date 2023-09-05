package org.genspectrum.ingest.utils

import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.parseObject
import com.alibaba.fastjson2.toJSONByteArray
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import java.io.*
import java.nio.file.Path
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun readFile(file: Path): InputStream {
    return ZstdInputStream(FileInputStream(file.toFile()))
}

fun writeFile(file: Path): OutputStream {
    val fileOut = FileOutputStream(file.toFile())
    val zstdOut = ZstdOutputStream(fileOut)
    return BufferedOutputStream(zstdOut)
}

inline fun <reified T> readNdjson(inputStream: InputStream, bufferQueueSize:Int = 100): Sequence<T> {
    return sequence {
        val lineQueue = LinkedBlockingQueue<String>(bufferQueueSize)
        val entryQueue = LinkedBlockingQueue<T>(bufferQueueSize)

        val fetchingThread = thread(start = true, name = "readNdjson.fetchingThread") {
            BufferedReader(InputStreamReader(inputStream), 524288).use { reader ->
                while (true) {
                    val nextEntry = reader.readLine() ?: break
                    lineQueue.put(nextEntry)
                }
            }
        }

        val parsingThread = thread(start = true, name = "readNdjson.parsingThread") {
            while (lineQueue.isNotEmpty() || fetchingThread.isAlive) {
                val nextLine = lineQueue.poll(1, TimeUnit.SECONDS) ?: continue
                val nextEntry = nextLine.parseObject<T>()
                entryQueue.put(nextEntry)
            }
        }

        while (entryQueue.isNotEmpty() || parsingThread.isAlive) {
            yield(entryQueue.poll(1, TimeUnit.SECONDS) ?: continue)
        }
    }
}

fun <T> writeNdjson(outputStream: OutputStream, bufferQueueSize: Int = 100): WriteNdjsonResponse<T> {
    val queue = LinkedBlockingQueue<T>(bufferQueueSize)
    var closed = false

    val writingThread = thread(start = true, name = "writeNdjson.writingThread") {
        outputStream.use { outputStream ->
            while (queue.isNotEmpty() || !closed) {
                val entry = queue.poll(1, TimeUnit.SECONDS) ?: continue
                outputStream.write(entry.toJSONByteArray(JSONWriter.Feature.WriteNulls))
                outputStream.write("\n".toByteArray())
            }
        }
    }

    return WriteNdjsonResponse(
        fun (entry: T) {
            queue.put(entry)
        },
        fun () {
            closed = true
            writingThread.join()
        }
    )
}

data class WriteNdjsonResponse<T> (
    val write: (T) -> Unit,
    val close: () -> Unit
)
