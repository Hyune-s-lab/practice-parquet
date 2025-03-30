package dev.hyunec.practiceparquet

import dev.hyunec.practiceparquet.service.ParquetReader
import dev.hyunec.practiceparquet.service.ParquetWriter
import dev.hyunec.practiceparquet.util.PerformanceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import java.io.File

@SpringBootApplication
class PracticeParquetApplication {
    private val log = KotlinLogging.logger {}

    @Bean
    fun commandLineRunner(
        parquetWriter: ParquetWriter,
        parquetReader: ParquetReader
    ): CommandLineRunner = CommandLineRunner {
        val inputFile = File("src/main/resources/input/sample.pdf")
        val fileName = inputFile.nameWithoutExtension
        val parquetFile = File("src/main/resources/persist/$fileName.parquet")

        log.info { "문서 파일 읽기 및 Parquet 파일로 변환 시작..." }
        log.info { "파일: ${inputFile.name}" }

        // MDC에 기본 정보 기록
        PerformanceLogger.recordMetric("입력_파일", inputFile.name)
        PerformanceLogger.recordMetric("출력_파일", parquetFile.name)
        PerformanceLogger.start("전체_프로세스")

        // 문서를 Parquet으로 변환
        parquetWriter.write(inputFile, parquetFile)
        log.info { "변환 완료: ${parquetFile.name}" }

        // Parquet 파일 읽기
        log.info { "Parquet 파일 읽기..." }
        val content = parquetReader.read(parquetFile)
        log.info { "Parquet 파일 읽기 완료 (${content.size}개 레코드)" }

        // 전체 프로세스 완료
        PerformanceLogger.end("전체_프로세스")

        // 성능 리포트 출력
        PerformanceLogger.printReport()
    }
}

fun main(args: Array<String>) {
    runApplication<PracticeParquetApplication>(*args)
}
