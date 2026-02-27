# DART Data Collection Pipeline

이 프로젝트는 DART 공시에서 2024 사업보고서 기반의 원문 사업내용과 재무 데이터를 JSON으로 수집합니다.

## 현재 수집 정책

- 기준 파일: `상장법인목록.xls` (KIND 상장법인 목록)
- 대상 식별자: 종목코드(숫자/영문 혼합 포함)
- 업종 저장: `industry_classification`에는 `"소분류"` 1개만 저장
- `"소분류"` 값: `상장법인목록.xls` D열(업종) 원문
- 제외 업종: D열이 `"금융 지원 서비스업"`인 기업은 수집 대상에서 제외

## 수집/제외 조건

- 수집 대상: `상장법인목록.xls`에 있는 기업(단, 제외 업종 제외)
- 제외 1: 2024 사업보고서가 없는 기업 (`skipped_no_2024_report`)
- 제외 2: 2024 사업보고서는 있으나 사업내용 섹션 추출 실패 기업 (`failed_business_content`)
- 규칙: 제외 2에 해당하면 JSON 파일을 생성하지 않고 실패 로그만 남김

## 출력 파일

- 기업 JSON: `data/<업종>/raw_<종목코드>.json`
- 진행 상태: `data/collection_status.json`
- 2024 보고서 미확보 로그: `missing_2024_reports.log`
- 사업내용 추출 실패 로그: `business_content_extraction_failures.log`
- 수동 처리용 목록
  - `MISSING_2024_BUSINESS_REPORTS.md`
  - `BUSINESS_CONTENT_EXTRACTION_FAILURES.md`

## 실행 방법

```bash
python collect_dart_manufacturing.py
```

체크포인트 기반이라 중간 중단 후 재실행 시 이어서 처리됩니다.

## 상태 요약 (collection_status 기준)

- 완료: `2582`
- 2024 사업보고서 없음: `62`
- 사업내용 추출 실패: `24`
- 조회 오류 스킵: `0`

