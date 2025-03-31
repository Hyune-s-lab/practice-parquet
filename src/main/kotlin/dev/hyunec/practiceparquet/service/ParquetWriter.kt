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
import org.springframework.stereotype.Service
import java.io.File

@Service
class ParquetWriter(private val documentParser: DocumentParser) {
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

        // 문서 파싱
        val (documents, stats) = documentParser.parseDocuments(listOf(documentFile))

        // Parquet 저장
        writeDocumentsToParquet(documents, outputParquetFile, stats.charCount)

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

        // 1. 문서 파싱
        val (allDocuments, documentStats) = documentParser.parseDocuments(documentFiles)

        // 2. Parquet 저장
        writeDocumentsToParquet(allDocuments, outputParquetFile, documentStats.charCount)

        PerformanceLogger.end("폴더_변환")
    }

    /**
     * Document 목록을 Parquet 파일로 저장
     */
    private fun writeDocumentsToParquet(documents: List<Document>, outputParquetFile: File, totalCharCount: Int) {
        // 스키마 생성
        val schema = createDocumentSchema()

        log.info { "2단계: Parquet 파일 저장 시작" }
        PerformanceLogger.start("Parquet_저장")
        writeToParquet(documents, schema, outputParquetFile)
        PerformanceLogger.end("Parquet_저장")
        log.info { "Parquet 파일 저장 완료: ${outputParquetFile.name}" }

        // 압축률 계산
        if (outputParquetFile.exists() && totalCharCount > 0) {
            val compressionRatio = outputParquetFile.length().toDouble() / totalCharCount.toDouble()
            PerformanceLogger.recordMetric("압축률_바이트_글자", String.format("%.2f", compressionRatio))
        }
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
