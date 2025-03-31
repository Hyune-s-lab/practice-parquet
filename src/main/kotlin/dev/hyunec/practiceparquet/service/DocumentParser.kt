package dev.hyunec.practiceparquet.service

import dev.hyunec.practiceparquet.model.DocumentStats
import dev.hyunec.practiceparquet.util.PerformanceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.ai.document.Document
import org.springframework.ai.reader.tika.TikaDocumentReader
import org.springframework.stereotype.Service
import java.io.File

@Service
class DocumentParser {
    private val log = KotlinLogging.logger {}

    /**
     * 문서 파일들을 파싱하여 Document 객체로 변환
     */
    fun parseDocuments(documentFiles: List<File>): Pair<List<Document>, DocumentStats> {
        log.info { "1단계: PDF 파일 파싱 시작 (총 ${documentFiles.size}개 파일)" }
        PerformanceLogger.start("PDF_파싱_전체")
        
        val allDocuments = mutableListOf<Document>()
        var totalLineCount = 0
        var totalCharCount = 0
        var totalWordCount = 0
        
        documentFiles.forEachIndexed { index, file ->
            log.info { "PDF 파싱 진행 중 (${index + 1}/${documentFiles.size}): ${file.name}" }
            
            val documents = readDocumentFile(file)
            allDocuments.addAll(documents)
            
            // 문서 내용 분석
            documents.forEach { document ->
                val textContent = document.text ?: ""
                totalLineCount += textContent.lines().size
                totalCharCount += textContent.length
                totalWordCount += textContent.split(Regex("\\s+")).size
            }
        }
        
        PerformanceLogger.end("PDF_파싱_전체")
        log.info { "PDF 파싱 완료: 총 ${allDocuments.size}개 문서" }
        
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
}
