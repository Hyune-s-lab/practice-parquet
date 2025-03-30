package dev.hyunec.practiceparquet.service

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
        log.info { "문서를 Parquet으로 변환: ${documentFile.name} -> ${outputParquetFile.name}" }

        // 기존 파일이 있으면 삭제
        if (outputParquetFile.exists()) {
            outputParquetFile.delete()
        }

        // 문서를 Parquet으로 변환
        val documents = readDocumentFile(documentFile)
        val schema = createDocumentSchema()
        writeToParquet(documents, schema, outputParquetFile)
    }

    /**
     * 문서 파일을 읽어 텍스트를 추출
     */
    private fun readDocumentFile(documentFile: File): List<Document> {
        log.info { "문서 파일 읽기: ${documentFile.name}" }
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
