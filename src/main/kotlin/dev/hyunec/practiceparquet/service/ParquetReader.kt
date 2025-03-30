package dev.hyunec.practiceparquet.service

import dev.hyunec.practiceparquet.util.PerformanceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.springframework.stereotype.Service
import java.io.File

@Service
class ParquetReader {
    private val log = KotlinLogging.logger {}

    /**
     * Parquet 파일 읽기
     */
    fun read(parquetFile: File): List<Map<String, String>> {
        log.info { "Parquet 파일 읽기 시작: ${parquetFile.name}" }
        PerformanceLogger.start("Parquet_읽기")

        val records = PerformanceLogger.measureWithValue("Parquet_레코드_읽기") {
            val configuration = Configuration()
            val path = Path(parquetFile.toString())
            val readSupport = GroupReadSupport()

            val records = mutableListOf<Map<String, String>>()

            ParquetReader.builder(readSupport, path)
                .withConf(configuration)
                .build().use { reader ->
                    generateSequence { reader.read() }
                        .takeWhile { true }
                        .forEach { group -> records.add(extractGroupData(group)) }
                }

            records
        }

        PerformanceLogger.recordMetric("Parquet_레코드_수", records.size)

        // 스키마 정보 기록
        val schema = getParquetSchema(parquetFile)
        val fieldsCount = schema.lines().count {
            it.contains("required") || it.contains("optional")
        }

        PerformanceLogger.recordMetric("Parquet_필드_수", fieldsCount)

        // 내용 샘플 기록 (로그만)
        if (records.isNotEmpty()) {
            val contentSample = records.firstOrNull()?.get("content")?.let {
                if (it.length > 50) it.substring(0, 50) + "..." else it
            } ?: "내용 없음"

            log.debug { "컨텐츠 샘플: $contentSample" }
        }

        PerformanceLogger.end("Parquet_읽기")

        return records
    }

    /**
     * Parquet Group에서 데이터 추출
     */
    private fun extractGroupData(group: Group): Map<String, String> {
        return listOf("id", "content", "metadata")
            .associateWith { field ->
                if (group.getFieldRepetitionCount(field) > 0) {
                    group.getString(field, 0)
                } else {
                    ""
                }
            }.filterValues { it.isNotEmpty() }
    }

    /**
     * Parquet 파일의 스키마 정보를 가져옵니다.
     */
    private fun getParquetSchema(parquetFile: File): String {
        val configuration = Configuration()
        val path = Path(parquetFile.toString())

        return ParquetFileReader.open(configuration, path).use { reader ->
            reader.footer.fileMetaData.schema.toString()
        }
    }
}
