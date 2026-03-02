# CLAUDE.md

이 파일은 이 저장소에서 작업하는 Claude Code(claude.ai/code)에게 가이드를 제공합니다.

## 프로젝트 개요

DART(금융감독원 전자공시시스템)에서 KRX 상장 기업의 2024년 사업보고서 데이터를 수집해 구조화된 JSON으로 저장하는 파이프라인입니다. 체크포인트 기반으로 중단 후 재실행해도 이어서 처리됩니다.

## 실행 방법

```bash
# 메인: DART 데이터 수집 (체크포인트 기반, 중단 후 재실행 안전)
python collect_dart_manufacturing.py

# create_master_table.py는 deprecated — 실행하거나 수정하지 말 것
```

**필수:** 프로젝트 루트의 `.env` 파일에 `DART_API_KEY`가 설정되어 있어야 합니다.

## 아키텍처

실질적인 스크립트는 `collect_dart_manufacturing.py` 하나입니다.

**입력:**
- `상장법인목록.xls` — KIND에서 내보낸 상장법인 목록 (HTML-table XLS, cp949 인코딩). 사용 컬럼: col[0]=회사명, col[2]=종목코드, col[3]=업종(소분류). 업종이 `"금융 지원 서비스업"`인 기업은 수집 대상에서 제외.
- `data/collection_status.json` — 각 종목코드의 처리 상태(`completed`, `skipped_no_2024_report`, `failed_business_content`, `skipped_lookup_error`)를 추적하는 체크포인트 파일.

**기업별 처리 순서:**
1. `find_2024_business_report()` — 종목코드로 DART 조회. 알파벳 포함 코드(예: KDR, ETF)는 `dart.find_corp_code()`로 corp_code를 조회해 fallback 처리.
2. `extract_business_description()` — `dart.sub_docs()`로 하위 문서 목록을 가져와 정규식(`BIZ_START_PATTERN` / `BIZ_END_PATTERN`)으로 사업내용 섹션을 찾고, 각 URL을 fetch해 HTML을 제거.
3. `fetch_financials_2023()` — `dart.finstate()`로 2023년 사업보고서(reprt_code `11011`) 재무 데이터 조회. CFS 우선, 없으면 OFS 사용. 예외는 `except: pass`로 묵음 처리됨.

**기업별 출력 파일:** `data/<업종소분류>/raw_<종목코드>.json`

```json
{
  "company_name": "...",
  "stock_code": "...",
  "bsns_year": "2024",
  "industry_classification": { "소분류": "..." },
  "business_description": "...",
  "financials": { "계정과목명": "금액", ... }
}
```

모든 JSON 파일은 UTF-8 인코딩입니다 (`ensure_ascii=False`).

## 체크포인트 / 상태 의미

- `completed` — JSON 저장 완료. 재실행 시 건너뜀.
- `skipped_no_2024_report` — DART에 2024 사업보고서 없음. 재실행 시 건너뜀.
- `skipped_lookup_error` — corp_code 조회 실패. 재실행 시 건너뜀.
- `failed_business_content` — 사업내용 섹션 미발견 또는 fetch 실패. **매 실행마다 재시도** (의도적 설계 — 네트워크 일시 오류가 많음).

특정 종목코드를 강제 재수집하려면 `data/collection_status.json`의 해당 상태 리스트에서 코드를 제거하면 됩니다.

## 알려진 문제 (IMPROVEMENT_BACKLOG.md 참고)

- **JSON 9건**: `business_description`이 빈 문자열 (구버전 수집 데이터. 조치: `status["completed"]`에서 해당 코드 제거).
- **JSON 201건**: `financials`가 빈 딕셔너리(`{}`) — `fetch_financials_2023()`이 모든 예외를 묵음 처리.
- **기업 24건**: `business_section_not_found` 실패 — `BIZ_START_PATTERN`이 해당 사업보고서의 섹션 제목과 불일치.
- `업종코드-11차표준산업분류.xlsx`가 프로젝트 루트에 존재하나 현재 코드에서 사용되지 않음.

## 주요 상수

```python
BIZ_START_PATTERN = r"사업의\s*내용|사업\s*현황"   # 사업내용 섹션 시작 탐지
BIZ_END_PATTERN   = r"재무에\s*관한\s*사항"          # 사업내용 섹션 끝 탐지
REPORT_PATTERN    = r"사업보고서.*\b2024\b"          # 2024 사업보고서 식별
INDUSTRY_KEY      = "소분류"                          # industry_classification 딕셔너리 키
```
