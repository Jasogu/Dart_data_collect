# DART Data Collection Pipeline

이 프로젝트는 오픈 API(DART)를 활용하여 한국거래소(유가증권/코스닥)에 상장된 기업들의 재무 데이터와 사업보고서 원문을 수집하는 파이프라인입니다. 

## 주요 기능

1. **상장사 필터링**: 
   - `상장법인목록.xls`를 기반으로 코스피/코스닥 상장사 중 최근(2023년 이전) 상장된 기업 추출
   - 특정 업종(예: 제조업 등) 키워드를 기반으로 대상 기업 선별
2. **원시 데이터 수집 (DART API)**:
   - 기업의 최신 재무 상태표(연결/개별 재무제표 기준 매출, 영업이익, 자산 등) 수집
   - 정기 공시된 사업보고서(기본 2024년 기준)에서 'II. 사업의 내용' 파트의 원문 텍스트 추출 및 HTML 태그 정제
3. **로컬 저장**:
   - 추출된 개별 기업의 데이터를 구조화된 JSON 포맷으로 `data/` 디렉토리에 저장

## 필수 요구 사항 (Prerequisites)

- Python 3.8+
- DART API Key (환경 변수 `.env` 기반)
- 아래 Python 패키지:
  ```bash
  pip install pandas python-dotenv OpenDartReader beautifulsoup4 lxml requests openpyxl xlrd
  ```

## 설정 및 실행 방법

1. **DART API Key 발급 및 설정**
   - [DART 오픈API](https://opendart.fss.or.kr/) 사이트에서 API 키를 발급받습니다.
   - 프로젝트 루트 디렉토리에 `.env` 파일을 생성하고 아래와 같이 입력합니다.
     ```
     DART_API_KEY=당신의_DART_API_키를_입력하세요
     ```

2. **기초 데이터 파일 준비**
   - 한국거래소(KIND)에서 다운로드한 `상장법인목록.xls` 파일이 프로젝트 루트에 존재해야 합니다.
   - (선택) `업종코드-표준산업분류 연계표_홈택스 게시.xlsx` 파일도 참고용으로 쓰일 수 있습니다.

3. **스크립트 실행**
   ```bash
   python collect_dart_manufacturing.py
   ```

## 향후 과제 (TODO)

- 전체 코스피/코스닥 상장사 대상 스케일업 수집
- DART API 호출 제한(Rate Limit) 및 타임아웃을 고려한 안정성 확보
- 대용량 데이터 적재를 위한 RDBMS (예: SQLite, MSSQL 등) 연동