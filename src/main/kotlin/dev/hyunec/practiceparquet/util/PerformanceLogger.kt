package dev.hyunec.practiceparquet.util

import io.github.oshai.kotlinlogging.KotlinLogging
import org.slf4j.MDC
import java.io.File
import kotlin.time.measureTimedValue

object PerformanceLogger {
    val log = KotlinLogging.logger {}

    // 작업 시작 시간 측정
    fun start(operation: String) {
        MDC.put("${operation}_start", System.currentTimeMillis().toString())
    }

    // 작업 종료 및 측정
    fun end(operation: String) {
        val startStr = MDC.get("${operation}_start") ?: return
        val start = startStr.toLongOrNull() ?: return
        val duration = System.currentTimeMillis() - start

        MDC.put("${operation}_duration", "${duration}ms")
        log.info { "$operation 완료 (${duration}ms)" }
    }

    // 메트릭 기록
    fun <T> recordMetric(name: String, value: T): T {
        MDC.put(name, value.toString())
        return value
    }

    // 값과 함께 측정
    inline fun <T> measureWithValue(operation: String, block: () -> T): T {
        val (result, duration) = measureTimedValue(block)
        MDC.put("${operation}_duration", "${duration.inWholeMilliseconds}ms")
        log.info { "$operation 완료 (${duration.inWholeMilliseconds}ms)" }
        return result
    }

    // 파일 처리 측정
    inline fun measureFile(operation: String, file: File, block: () -> Unit) {
        start(operation)
        block()
        end(operation)

        if (file.exists()) {
            val size = file.length()
            recordMetric("${operation}_size", formatFileSize(size))
        }
    }

    // 성능 리포트 출력
    fun printReport() {
        log.info { "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" }
        log.info { "📊 성능 측정 리포트" }
        log.info { "───────────────────────────────────────────" }

        val metrics = MDC.getCopyOfContextMap() ?: mapOf()
        val durations = metrics.filterKeys { it.endsWith("_duration") }
            .mapKeys { it.key.removeSuffix("_duration") }
            .toSortedMap()

        if (durations.isNotEmpty()) {
            log.info { "⏱️ 실행 시간:" }
            durations.forEach { (op, time) ->
                log.info { "  ▪ $op: $time" }
            }
        }

        val otherMetrics = metrics.filterKeys { !it.endsWith("_duration") && !it.endsWith("_start") }
            .toSortedMap()
        if (otherMetrics.isNotEmpty()) {
            log.info { "📈 측정 메트릭:" }
            otherMetrics.forEach { (name, value) ->
                log.info { "  ▪ $name: $value" }
            }
        }

        log.info { "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" }
    }

    // 모든 측정 데이터 초기화
    fun reset() {
        MDC.clear()
    }

    // 바이트 포맷팅
    fun formatFileSize(bytes: Long): String = when {
        bytes < 1024 -> "$bytes B"
        bytes < 1024 * 1024 -> String.format("%.2f KB", bytes / 1024.0)
        bytes < 1024 * 1024 * 1024 -> String.format("%.2f MB", bytes / (1024.0 * 1024.0))
        else -> String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}
