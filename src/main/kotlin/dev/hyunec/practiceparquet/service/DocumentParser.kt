package dev.hyunec.practiceparquet.service

import dev.hyunec.practiceparquet.model.DocumentStats
import dev.hyunec.practiceparquet.util.PerformanceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.springframework.ai.document.Document
import org.springframework.ai.reader.tika.TikaDocumentReader
import org.springframework.stereotype.Service
import java.io.File
import java.util.concurrent.Executors

@Service
class DocumentParser {
    private val log = KotlinLogging.logger {}

    /**
     * 문서 파일들을 파싱하여 Document 객체로 변환 (가상 스레드 기반 병렬 처리)
     */
    fun parseDocuments(documentFiles: List<File>): Pair<List<Document>, DocumentStats> {
        log.info { "1단계: PDF 파일 병렬 파싱 시작 (총 ${documentFiles.size}개 파일)" }
        PerformanceLogger.start("PDF_파싱_전체")

        val allDocuments = mutableListOf<Document>()
        var totalLineCount = 0
        var totalCharCount = 0
        var totalWordCount = 0

        // 가상 스레드 기반 스레드 풀 생성
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            // 파일별 작업 목록 생성
            val parseResults = documentFiles.mapIndexed { index, file ->
                executor.submit<ParseResult> {
                    log.info { "PDF 파싱 시작 (${index + 1}/${documentFiles.size}): ${file.name}" }
                    try {
                        val documents = readDocumentFile(file)

                        // 문서 통계 계산
                        var fileLineCount = 0
                        var fileCharCount = 0
                        var fileWordCount = 0

                        documents.forEach { document ->
                            val textContent = document.text ?: ""
                            fileLineCount += textContent.lines().size
                            fileCharCount += textContent.length
                            fileWordCount += textContent.split(Regex("\\s+")).size
                        }

                        log.info { "PDF 파싱 완료 (${index + 1}/${documentFiles.size}): ${file.name}" }

                        ParseResult(documents, fileLineCount, fileCharCount, fileWordCount)
                    } catch (e: Exception) {
                        log.error(e) { "파일 파싱 오류: ${file.name}" }
                        ParseResult(emptyList(), 0, 0, 0)
                    }
                }
            }

            // 모든 작업 결과 수집
            parseResults.forEach { future ->
                val result = future.get()
                synchronized(allDocuments) {
                    allDocuments.addAll(result.documents)
                    totalLineCount += result.lineCount
                    totalCharCount += result.charCount
                    totalWordCount += result.wordCount
                }
            }
        }

        // 코루틴 병렬 처리 추가 (가상 스레드와 함께 사용)
        runBlocking {
            val additionalJobs = documentFiles.chunked(100).map { fileChunk ->
                async(Dispatchers.IO) {
                    // 여기서 추가 메타데이터 처리나 향상된 분석 작업 수행 가능
                    // 실제 파싱은 위에서 이미 완료됨
                    log.debug { "추가 메타데이터 처리: ${fileChunk.size}개 파일" }
                }
            }
            additionalJobs.awaitAll()
        }

        PerformanceLogger.end("PDF_파싱_전체")
        log.info { "PDF 병렬 파싱 완료: 총 ${allDocuments.size}개 문서" }

        // 통계 기록
        val stats = DocumentStats(
            documentCount = allDocuments.size,
            lineCount = totalLineCount,
            charCount = totalCharCount,
            wordCount = totalWordCount
        )

        PerformanceLogger.recordMetric("전체_문서_개수", stats.documentCount)
        PerformanceLogger.recordMetric("전체_라인_수", stats.lineCount)
        PerformanceLogger.recordMetric("전체_글자_수", stats.charCount)
        PerformanceLogger.recordMetric("전체_단어_수", stats.wordCount)

        return Pair(allDocuments, stats)
    }

    /**
     * 문서 파일을 읽어 텍스트를 추출
     */
    private fun readDocumentFile(documentFile: File): List<Document> {
        if (!documentFile.exists()) {
            throw IllegalArgumentException("파일이 존재하지 않습니다: ${documentFile.name}")
        }

        val reader = TikaDocumentReader(documentFile.toURI().toString())
        return reader.get()
    }

    /**
     * 파싱 결과 데이터 클래스
     */
    private data class ParseResult(
        val documents: List<Document>,
        val lineCount: Int,
        val charCount: Int,
        val wordCount: Int
    )
}
