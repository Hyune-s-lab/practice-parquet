package dev.hyunec.practiceparquet.service

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
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
        log.info { "Parquet 파일 읽기: ${parquetFile.name}" }
        val configuration = Configuration()
        val path = Path(parquetFile.toString())
        val results = mutableListOf<Map<String, String>>()
        val readSupport = GroupReadSupport()

        ParquetReader.builder(readSupport, path)
            .withConf(configuration)
            .build().use { reader ->
                generateSequence { reader.read() }
                    .takeWhile { true }
                    .forEach { group -> results.add(extractGroupData(group)) }
            }

        return results
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
}
