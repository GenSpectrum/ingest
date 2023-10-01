package org.genspectrum.ingest.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

fun <T> runParallel(tasks: List<() -> T>, maxThreads: Int): List<T> {
    val semaphore = Semaphore(maxThreads)
    return runBlocking(Dispatchers.Default) {
        tasks.map { task ->
            async {
                semaphore.withPermit {
                    task.invoke()
                }
            }
        }.awaitAll()
    }
}
