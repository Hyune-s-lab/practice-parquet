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
class ParquetSearcher {
    private val log = KotlinLogging.logger {}

    /**
     * Parquet 파일에서 키워드 검색
     * @param parquetFile 검색할 Parquet 파일
     * @param keyword 검색 키워드
     * @return 키워드를 포함하는 문서 목록
     */
    fun search(parquetFile: File, keyword: String): List<Map<String, String>> {
        log.info { "Parquet 파일 키워드 검색 시작: ${parquetFile.name}, 키워드: $keyword" }
        PerformanceLogger.start("Parquet_검색")

        val matchedDocuments = mutableListOf<Map<String, String>>()
        val configuration = Configuration()
        val path = Path(parquetFile.toString())

        // 파킷 파일 메타데이터 정보 확인
        val metadata = ParquetFileReader.readFooter(configuration, path)
        log.info { "Parquet 파일 정보: ${metadata.fileMetaData.keyValueMetaData}" }
        log.info { "총 로우 그룹 수: ${metadata.blocks.size}" }

        try {
            // 키워드 검색 전략 1: 전체 파일 스캔 (더 정확한 검색)
            val startTime = System.currentTimeMillis()
            val reader = ParquetReader.builder(GroupReadSupport(), path)
                .withConf(configuration)
                .build()

            var group: Group? = reader.read()
            var count = 0

            while (group != null) {
                val content = group.getString("content", 0)

                if (content.contains(keyword, ignoreCase = true)) {
                    val id = group.getString("id", 0)
                    val metadata = group.getString("metadata", 0)

                    val document = mapOf(
                        "id" to id,
                        "content" to content,
                        "metadata" to metadata
                    )

                    matchedDocuments.add(document)
                }

                count++
                group = reader.read()
            }

            reader.close()

            val endTime = System.currentTimeMillis()
            log.info { "검색 완료: ${matchedDocuments.size}개 문서 발견 (총 ${count}개 검색, ${endTime - startTime}ms 소요)" }
        } catch (e: Exception) {
            log.error(e) { "Parquet 검색 중 오류 발생" }
        }

        PerformanceLogger.end("Parquet_검색")
        PerformanceLogger.recordMetric("검색_키워드", keyword)
        PerformanceLogger.recordMetric("검색_결과_수", matchedDocuments.size)

        return matchedDocuments
    }

    /**
     * 고급 검색: 여러 필드와 조건으로 검색 (향후 확장용)
     */
    fun advancedSearch(parquetFile: File, criteria: Map<String, String>): List<Map<String, String>> {
        // 향후 구현
        return emptyList()
    }
}
