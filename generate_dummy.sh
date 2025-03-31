#!/bin/bash

# 인자 확인
if [ $# -ne 2 ]; then
  echo "사용법: $0 <원본파일명> <복사횟수>"
  exit 1
fi

ORIGIN_FILE=$1
COUNT=$2

# 원본 파일 존재 확인
if [ ! -f "$ORIGIN_FILE" ]; then
  echo "에러: 원본 파일 '$ORIGIN_FILE'이 존재하지 않습니다."
  exit 1
fi

# 숫자 확인
if ! [[ "$COUNT" =~ ^[0-9]+$ ]]; then
  echo "에러: 복사 횟수는 정수여야 합니다."
  exit 1
fi

# 파일 이름과 확장자 분리
FILENAME=$(basename -- "$ORIGIN_FILE")
EXT="${FILENAME##*.}"
NAME="${FILENAME%.*}"

# 원본 파일의 디렉토리 경로 추출
DIR_PATH=$(dirname "$ORIGIN_FILE")

# 같은 패턴의 기존 파일들 삭제 (원본 제외)
find "$DIR_PATH" -type f -name "${NAME}_copy*.${EXT}" -delete

# 지정된 횟수만큼 파일 복사
for i in $(seq 1 "$COUNT"); do
  NEW_FILE="$DIR_PATH/${NAME}_copy${i}.${EXT}"
  cp "$ORIGIN_FILE" "$NEW_FILE"
done
