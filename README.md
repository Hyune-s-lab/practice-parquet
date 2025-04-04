# Practice Parquet

PDF 파일을 읽고 Parquet 형식으로 변환하는 Spring Boot 애플리케이션입니다.

### dependencies

- Spring Boot 3.4.4
- Spring AI Tika Document Reader 1.0.0-M6
- Apache Parquet 1.13.1
- Apache Hadoop Client 3.3.6

## phase 1: pdf to parquet

1. `src/main/resources/input` 경로에 샘플 파일 복사
    - https://examplefile.com/document/pdf
2. 원하는 샘플 파일 개수 생성

```shell
./generate_dummy.sh {{origin_file}} {{count}}
./generate_dummy.sh src/main/resources/input/1mb.pdf 3
```

## phase 2: keyword search

1. keyword 입력

```kotlin
val searchKeyword = "L034" // 실제 파일에 있을만한 키워드로 변경 필요
```

2. 실행

<img width="1521" alt="Image" src="https://github.com/user-attachments/assets/51731da5-9267-41b9-81b0-cb8f65845a7c" />
