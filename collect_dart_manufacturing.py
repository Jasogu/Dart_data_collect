import os
import re
import json
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import OpenDartReader
from bs4 import BeautifulSoup

# --- 1. 설정 및 초기화 ---
load_dotenv()
DART_API_KEY = os.getenv("DART_API_KEY")
if not DART_API_KEY:
    print("Error: DART_API_KEY is not set in environment.")
    exit(1)

dart = OpenDartReader(DART_API_KEY)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

MASTER_FILE = DATA_DIR / "stock_master_list.csv"
STATUS_FILE = DATA_DIR / "collection_status.json"

# --- 2. 텍스트 정제 유틸 ---
def remove_html_tags_and_whitespace(html_text: str) -> str:
    """html 텍스트에서 태그를 모두 제거하고 평문으로 변환"""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, 'lxml')
    text = soup.get_text(separator=' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# --- 3. 상태 관리 (Checkpoint) ---
def load_status():
    if STATUS_FILE.exists():
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"completed": []}

def save_status(status_data):
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2)

# --- 4. 메인 데이터 수집 로직 ---
def fetch_dart_data():
    if not MASTER_FILE.exists():
        print(f"Error: {MASTER_FILE} not found. Run create_master_table.py first.")
        return
        
    df_master = pd.read_csv(MASTER_FILE, dtype=str)
    
    # NaN 값 빈 문자열로 처리
    df_master = df_master.fillna("")
    
    # 상태 로드
    status = load_status()
    completed_codes = set(status.get("completed", []))
    
    print(f"[Info] 전체 타겟 상장사: {len(df_master)} 개")
    print(f"[Info] 이미 수집 완료된 상장사: {len(completed_codes)} 개")
    
    # (사용자 요청) 우선 100개 그룹으로 나누어 수집 테스트 진행
    limit_count = 100
    current_count = 0
    
    for idx, row in df_master.iterrows():
        if current_count >= limit_count:
             print(f"[Info] 지정된 테스트 횟수({limit_count})를 달성하여 종료합니다.")
             break
             
        comp_name = str(row.get('회사명', '')).strip()
        code = str(row.get('종목코드', '')).zfill(6)
        dae_class = str(row.get('대분류', '')).strip()
        dart_industry_name = str(row.get('세세분류_명', '')).strip() # 상장법인목록 원본 '업종' 컬럼
        
        # 1. 예외 처리: 금융 및 보험업은 수집 대상에서 제외
        if dae_class == "금융 및 보험업":
            print(f"[{idx+1}/{len(df_master)}] {comp_name} ({code}) -> 금융 및 보험업이므로 수집 제외")
            continue
        
        if not code or code == "000nan":
            continue
            
        if code in completed_codes:
            continue
            
        # 2. 디렉토리 생성을 위한 폴더명 정제 (특수문자 치환)
        # 사용자 요청: 폴더는 상장법인목록의 '업종' 컬럼(마스터 테이블의 '세세분류_명' 값)을 기준으로 생성
        def clean_dir_name(name):
            if not name or name == "미분류":
                return "기타"
            return re.sub(r'[/\\?%*:|"<>]', '_', name)
            
        industry_folder_name = clean_dir_name(dart_industry_name)
        
        # 저장될 타겟 폴더: data / [업종명]
        target_dir = DATA_DIR / industry_folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"[{idx+1}/{len(df_master)}] Fetching {comp_name} ({code})... -> {industry_folder_name}")
        
        # 메타데이터 패키징
        data_packet = {
            "company_name": comp_name,
            "stock_code": code,
            "bsns_year": "2024",
            "industry_classification": {
                "대분류": row.get('대분류', ''),
                "중분류": row.get('중분류', ''),
                "소분류": row.get('소분류', ''),
                "세분류": row.get('세분류', ''),
                "세세분류_코드": row.get('세세분류_코드', ''),
                "세세분류_명": row.get('세세분류_명', '')
            },
            "business_description": "",
            "financials": {}
        }
        
        current_count += 1
        
        # 1. 재무 데이터 수집
        try:
            fs_data = dart.finstate(code, bsns_year="2023", reprt_code="11011") 
            if fs_data is not None and not (isinstance(fs_data, dict) and fs_data.get('status')):
                if not fs_data.empty:
                    cfs_df = fs_data[fs_data['fs_div'] == 'CFS']
                    if cfs_df.empty:
                        cfs_df = fs_data[fs_data['fs_div'] == 'OFS']
                        
                    for _, fs_row in cfs_df.iterrows():
                        acc_nm = fs_row.get('account_nm')
                        amount = fs_row.get('thstrm_amount')
                        if acc_nm and amount:
                            data_packet["financials"][acc_nm] = amount
        except Exception as e:
            pass # 재무 데이터 없음 무시
            
        # 2. 사업의 내용 문서 원문 획득
        has_2024_report = False
        try:
            # 2024년 1월 1일 이후 제출된 보고서 목록 조회
            list_df = dart.list(code, start="20240101", kind="A")
            if list_df is not None and not list_df.empty:
               # 2024년 결산기에 대한 "사업보고서 (2024.12)" 정규식 패턴 탐색
               # 대체로 2025년 3월에 제출되므로 report_nm에 2024가 포함되는지 확인
               biz_reports = list_df[list_df['report_nm'].str.contains(r'사업보고서.*\b2024\b')]
               
               if not biz_reports.empty:
                   has_2024_report = True
                   rcept_no = biz_reports.iloc[0]['rcept_no']
                   sub_docs = dart.sub_docs(rcept_no)
                   
                   if sub_docs is not None and not sub_docs.empty:
                       biz_start_idx, biz_end_idx = -1, -1
                       
                       for i, sub_row in sub_docs.iterrows():
                           title = str(sub_row['title']).strip()
                           if biz_start_idx == -1 and bool(re.search(r'사업의\s*내용|사업\s*현황', title)):
                               biz_start_idx = i
                           elif biz_start_idx != -1:
                               if bool(re.search(r'재무에\s*관한\s*사항', title)) or bool(re.match(r'^(III|IV|V|VI|VII)\.', title)):
                                   biz_end_idx = i
                                   break
                       
                       if biz_start_idx != -1:
                           if biz_end_idx == -1:
                               biz_end_idx = len(sub_docs)
                               
                           combined_texts = []
                           for i in range(biz_start_idx, biz_end_idx):
                               doc_url = sub_docs.iloc[i]['url']
                               try:
                                   import requests
                                   resp = requests.get(doc_url)
                                   if resp.status_code == 200:
                                       plain_text = remove_html_tags_and_whitespace(resp.text)
                                       combined_texts.append(plain_text)
                               except Exception as e:
                                   pass
                                   
                           data_packet["business_description"] = "\n\n".join(combined_texts)
        except Exception as e:
           pass
           
        if not has_2024_report:
           print(f"  └ [Warning] {comp_name} ({code}): 2024년 사업보고서(2024 기재)를 찾을 수 없습니다.")
           with open("missing_2024_reports.log", "a", encoding="utf-8") as log_f:
                log_f.write(f"{code},{comp_name},2024년 사업보고서 미제출(또는 검색 실패)\n")
           
        # 3. JSON 덤프 저장
        file_path = target_dir / f"raw_{code}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data_packet, f, ensure_ascii=False, indent=2)
            
        # 상태 업데이트 및 저장
        completed_codes.add(code)
        status['completed'] = list(completed_codes)
        save_status(status)
        
        time.sleep(1) # Rate limiting 방어 (DART는 가혹함)

if __name__ == "__main__":
    fetch_dart_data()
