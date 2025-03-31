package dev.hyunec.practiceparquet.model

/**
 * 문서 통계 정보 클래스
 */
data class DocumentStats(
    val documentCount: Int,
    val lineCount: Int,
    val charCount: Int,
    val wordCount: Int
)
