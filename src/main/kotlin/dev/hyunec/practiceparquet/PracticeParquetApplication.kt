package dev.hyunec.practiceparquet

import dev.hyunec.practiceparquet.service.ParquetSearcher
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
        parquetSearcher: ParquetSearcher
    ): CommandLineRunner = CommandLineRunner {
        val inputFolder = File("src/main/resources/input")
        val outputFolder = File("src/main/resources/persist")

        // 출력 폴더가 없으면 생성
        if (!outputFolder.exists()) {
            outputFolder.mkdirs()
        }

        // 폴더 내 모든 파일 목록 가져오기
        val inputFiles = inputFolder.listFiles { file -> file.isFile } ?: emptyArray()

        log.info { "문서 폴더 읽기 및 Parquet 파일로 변환 시작..." }
        log.info { "파일 개수: ${inputFiles.size}" }

        // 폴더명을 기반으로 단일 parquet 파일명 결정
        val folderName = inputFolder.name
        val parquetFile = File(outputFolder, "$folderName.parquet")

        // MDC에 기본 정보 기록
        PerformanceLogger.recordMetric("입력_폴더", inputFolder.absolutePath)
        PerformanceLogger.recordMetric("파일_개수", inputFiles.size)
        PerformanceLogger.recordMetric("출력_파일", parquetFile.name)
        PerformanceLogger.start("전체_프로세스")

        // 폴더 내 모든 문서를 하나의 Parquet으로 변환
        parquetWriter.writeFolder(inputFiles.toList(), parquetFile)
        log.info { "변환 완료: ${parquetFile.name}" }

        // 키워드 검색 테스트
        log.info { "Parquet 파일 키워드 검색 테스트..." }
        val searchKeyword = "L034" // 실제 파일에 있을만한 키워드로 변경 필요
        val searchResults = parquetSearcher.search(parquetFile, searchKeyword)
        log.info { "검색 결과: ${searchResults.size}개 문서에서 '$searchKeyword' 발견" }

        // 검색 결과 샘플 출력
        if (searchResults.isNotEmpty()) {
            val sample = searchResults.first()
            log.info { "검색 결과 샘플 - ID: ${sample["id"]}" }
            log.info { "내용 미리보기: ${sample["content"]?.take(100) ?: "[내용 없음]"}..." }
        }

        // 전체 프로세스 완료
        PerformanceLogger.end("전체_프로세스")

        // 성능 리포트 출력
        PerformanceLogger.printReport()
    }
}

fun main(args: Array<String>) {
    runApplication<PracticeParquetApplication>(*args)
}
