# 개선 사항 백로그

- 작성일: 2026-02-28
- 현재 수집 현황: 완료 2,582 / 보고서 없음 62 / 사업내용 추출 실패 24 / 조회 오류 0

---

## P1 — 데이터 재수집 (즉시 처리)

### 1-A. business_description 공백 JSON 9건 재수집

- 상세: `EMPTY_BUSINESS_DESCRIPTION.md` 참고
- 현상: JSON 파일은 존재하고 `status["completed"]`에 등록돼 있으나 `business_description`이 빈 문자열
- 원인: 이전 실행 버전 차이로 빈 값이 저장된 것으로 추정 (현재 코드는 재현 불가)
- 조치: `status["completed"]`에서 해당 9개 종목코드 제거 → 다음 실행 시 자동 재수집

### 1-B. financials 미수집 201건 원인 파악 및 재수집 검토

- 상세: `EMPTY_FINANCIALS.md` 참고
- 현상: `financials: {}` 빈 딕셔너리로 저장됨 (전체의 7.7%)
- 원인 불명확: `fetch_financials_2023()` 내 `except: pass`로 오류가 묵음 처리됨
- 조치 후보:
  1. `except: pass` → 오류 로깅 추가 후 재실행해 실제 원인 확인
  2. 원인 확인 후 재수집 또는 결측 표기 정책 결정

---

## P2 — 사업내용 추출 실패 24건 패턴 개선

- 상세: `BUSINESS_CONTENT_EXTRACTION_FAILURES.md` 참고
- 현상: 24건 전부 `business_section_not_found` 동일 사유
- 원인: `BIZ_START_PATTERN`이 일부 사업보고서의 섹션 제목과 불일치
  ```python
  BIZ_START_PATTERN = r"사업의\s*내용|사업\s*현황"
  ```
- 조치: 실패 건의 실제 sub_docs 제목을 수동 확인 → 패턴 보강 또는 예외 처리 추가

---

## P3 — 코드 품질

### 3-A. `fetch_financials_2023()` 오류 묵음 제거

- 현상: `except: pass`로 모든 예외를 무시
- 조치: 최소한 실패 사유를 JSON 필드(`financials_error`)에 기록하거나 별도 로그 추가

### 3-B. alpha 코드 fallback 조건 취약

- 현상: `err.startswith("dart_list_error:ValueError")`로 특정 에러 문자열에 의존
- 위험: OpenDartReader 버전 업 시 에러 메시지 변경으로 fallback 무력화 가능
- 조치: 조건을 종목코드 형식 기반으로 단순화 (알파 포함 여부 선행 체크)

### 3-C. `create_master_table.py` 삭제

- 현상: 실행 시 deprecation 메시지 출력 후 종료 — 기능 없음
- 조치: 파일 삭제

---

## P4 — 정리 및 문서화

### 4-A. `docs_cache/` pkl 파일 정리

- 현상: `20260226.pkl`이 git에서 삭제 상태, `20260227.pkl`은 미추적 상태
- 조치: `.gitignore`에 `docs_cache/` 추가 또는 최신 파일만 유지

### 4-B. `업종코드-11차표준산업분류.xlsx` 활용 여부 결정

- 현상: 프로젝트 루트에 존재하나 현재 코드에서 참조하지 않음
- 조치: 향후 업종 정규화 시 활용 계획이 없다면 삭제 또는 `.gitignore` 처리

---

## 참고: 현재 미해결 건 요약

| 구분 | 수량 | 관련 파일 |
|---|---|---|
| business_description 공백 JSON | 9 | `EMPTY_BUSINESS_DESCRIPTION.md` |
| financials 빈 딕셔너리 | 201 | `EMPTY_FINANCIALS.md` |
| 사업내용 추출 실패 (section not found) | 24 | `BUSINESS_CONTENT_EXTRACTION_FAILURES.md` |
| 2024 사업보고서 미존재 | 62 | `MISSING_2024_BUSINESS_REPORTS.md` |
