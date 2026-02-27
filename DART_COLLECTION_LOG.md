# DART Collection Log

## 2026-02-27 업데이트 요약

- 수집 스크립트 기준을 `상장법인목록.xls` 중심으로 정리
- 종목코드 영문/숫자 혼합 코드 지원 (필요 시 `corp_code` fallback 조회)
- `industry_classification`은 `"소분류"`만 저장
- D열이 `"금융 지원 서비스업"`인 기업은 수집 대상에서 완전 제외
- 2024 사업보고서 미존재 기업은 JSON 미생성 + 로그 기록
- 사업내용 추출 실패 기업은 JSON 미생성 + 로그 기록

## 현재 결과 (collection_status 기준)

- 완료: `2582`
- 2024 사업보고서 없음: `62`
- 사업내용 추출 실패: `24`
- 조회 오류 스킵: `0`

## 운영 산출물

- 상태 파일: `data/collection_status.json`
- 보고서 없음 로그: `missing_2024_reports.log`
- 사업내용 실패 로그: `business_content_extraction_failures.log`
- 수동 점검용 목록:
  - `MISSING_2024_BUSINESS_REPORTS.md`
  - `BUSINESS_CONTENT_EXTRACTION_FAILURES.md`

## 비고

- 과거 실행에서 생성된 `"금융 지원 서비스업"` JSON은 정리하여 제거함.
- 남은 실패 건은 대부분 `business_section_not_found` 사유로, 개별 수동 수집/보정 대상임.

