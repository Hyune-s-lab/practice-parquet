package dev.hyunec.practiceparquet.service

import dev.hyunec.practiceparquet.util.PerformanceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.springframework.ai.document.Document
import org.springframework.ai.reader.tika.TikaDocumentReader
import org.springframework.stereotype.Service
import java.io.File

@Service
class ParquetWriter {
    private val log = KotlinLogging.logger {}

    /**
     * 문서 파일을 Parquet으로 변환
     */
    fun write(documentFile: File, outputParquetFile: File) {
        log.info { "문서를 Parquet으로 변환 시작: ${documentFile.name} -> ${outputParquetFile.name}" }
        PerformanceLogger.start("문서_변환")

        // 기존 파일이 있으면 삭제
        if (outputParquetFile.exists()) {
            outputParquetFile.delete()
        }

        // 문서 읽기
        val documents = PerformanceLogger.measureWithValue("문서_읽기") {
            readDocumentFile(documentFile)
        }

        // 문서 내용 분석
        val textContent = documents.firstOrNull()?.text ?: ""
        val lineCount = textContent.lines().size
        val charCount = textContent.length
        val wordCount = textContent.split(Regex("\\s+")).size
        
        PerformanceLogger.recordMetric("문서_개수", documents.size)
        PerformanceLogger.recordMetric("문서_라인_수", lineCount)
        PerformanceLogger.recordMetric("문서_글자_수", charCount)
        PerformanceLogger.recordMetric("문서_단어_수", wordCount)

        // 스키마 생성
        val schema = createDocumentSchema()

        // Parquet 파일 쓰기
        PerformanceLogger.measureFile("Parquet_쓰기", outputParquetFile) {
            writeToParquet(documents, schema, outputParquetFile)
        }
        
        // 압축률 계산
        if (outputParquetFile.exists() && charCount > 0) {
            val compressionRatio = outputParquetFile.length().toDouble() / charCount.toDouble()
            PerformanceLogger.recordMetric("압축률_바이트_글자", String.format("%.2f", compressionRatio))
        }
        
        PerformanceLogger.end("문서_변환")
    }

    /**
     * 폴더 내 모든 문서 파일을 하나의 Parquet으로 변환
     */
    fun writeFolder(documentFiles: List<File>, outputParquetFile: File) {
        log.info { "폴더 내 문서를 하나의 Parquet으로 변환 시작: ${documentFiles.size}개 파일 -> ${outputParquetFile.name}" }
        PerformanceLogger.start("폴더_변환")

        // 기존 파일이 있으면 삭제
        if (outputParquetFile.exists()) {
            outputParquetFile.delete()
        }

        // 스키마 생성
        val schema = createDocumentSchema()

        // 모든 문서 읽기 및 합치기
        val allDocuments = mutableListOf<Document>()
        var totalLineCount = 0
        var totalCharCount = 0
        var totalWordCount = 0
        
        documentFiles.forEachIndexed { index, file ->
            log.info { "문서 읽기 진행 중 (${index + 1}/${documentFiles.size}): ${file.name}" }
            
            PerformanceLogger.measureWithValue("문서_읽기_${file.name}") {
                val documents = readDocumentFile(file)
                allDocuments.addAll(documents)
                
                // 문서 내용 분석
                documents.forEach { document ->
                    val textContent = document.text ?: ""
                    totalLineCount += textContent.lines().size
                    totalCharCount += textContent.length
                    totalWordCount += textContent.split(Regex("\\s+")).size
                }
                
                documents
            }
        }
        
        // 통계 기록
        PerformanceLogger.recordMetric("전체_문서_개수", allDocuments.size)
        PerformanceLogger.recordMetric("전체_라인_수", totalLineCount)
        PerformanceLogger.recordMetric("전체_글자_수", totalCharCount)
        PerformanceLogger.recordMetric("전체_단어_수", totalWordCount)

        log.info { "총 ${allDocuments.size}개 문서 읽기 완료. Parquet 파일 생성 중..." }

        // Parquet 파일 쓰기
        PerformanceLogger.measureFile("Parquet_쓰기", outputParquetFile) {
            writeToParquet(allDocuments, schema, outputParquetFile)
        }
        
        // 압축률 계산
        if (outputParquetFile.exists() && totalCharCount > 0) {
            val compressionRatio = outputParquetFile.length().toDouble() / totalCharCount.toDouble()
            PerformanceLogger.recordMetric("압축률_바이트_글자", String.format("%.2f", compressionRatio))
        }
        
        PerformanceLogger.end("폴더_변환")
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
     * Document 스키마 생성
     */
    private fun createDocumentSchema(): MessageType {
        return MessageTypeParser.parseMessageType(
            """
            message document {
              required binary id (UTF8);
              required binary content (UTF8);
              required binary metadata (UTF8);
            }
        """.trimIndent()
        )
    }

    /**
     * 문서 목록을 Parquet 파일로 저장
     */
    private fun writeToParquet(documents: List<Document>, schema: MessageType, outputFile: File) {
        val configuration = Configuration()
        val path = Path(outputFile.toString())

        ExampleParquetWriter.builder(path)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withType(schema)
            .withConf(configuration)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .build().use { writer ->
                val factory = SimpleGroupFactory(schema)

                documents.forEach { document ->
                    val group = factory.newGroup()
                    group.add("id", document.id)
                    group.add("content", document.text)
                    group.add("metadata", document.metadata.toString())

                    writer.write(group)
                }
            }
    }
}
