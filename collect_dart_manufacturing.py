import os
import re
import json
import time
from datetime import datetime
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

# 원시 데이터 저장 디렉토리
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# (테스트용) 기존 json 파일 초기화 삭제
import glob
for f_path in DATA_DIR.glob("*.json"):
    try:
        f_path.unlink()
    except Exception as e:
        print(f"Failed to delete {f_path}: {e}")

# --- 2. 텍스트 정제 유틸 ---
def remove_html_tags_and_whitespace(html_text: str) -> str:
    """html 텍스트에서 태그를 모두 제거하고 평문으로 변환"""
    if not html_text:
        return ""
    # BeautifulSoup으로 태그 제거
    soup = BeautifulSoup(html_text, 'lxml')
    # 텍스트 추출 (구분자는 공백 한 칸)
    text = soup.get_text(separator=' ')
    # 연속된 공백, 줄바꿈, 탭을 단일 공백으로 치환
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# --- 3. 필터링 로직: 제조 상장사 추출 ---
def get_manufacturing_companies():
    """상장법인목록.xls와 업종코드 연계표를 조합해 제조업 상장사 리스트만 추출"""
    
    # 3.1 상장법인(유가, 코스닥) 파싱
    listed_file = "상장법인목록.xls"
    if not os.path.exists(listed_file):
        raise FileNotFoundError(f"{listed_file} not found. Please download from KIND.")
    
    try:
        # EUC-KR / CP949 인코딩 HTML 표
        df_listed = pd.read_html(listed_file, encoding='cp949')[0]
    except Exception as e:
        raise RuntimeError(f"Failed to parse {listed_file} HTML table. Ex: {e}")
        
    df_listed.columns = ['회사명', '소속부', '종목코드', '업종', '주요제품', '상장일', '결산월', '대표자명', '홈페이지', '지역']
    
    # 소속부: 코스피(유가증권), 코스닥만 포함
    allowed_markets = ['유가증권', '코스닥']
    df_listed = df_listed[df_listed['소속부'].isin(allowed_markets)].copy()
    
    # [추가] 2023년 이전 상장 기업만 필터링 (2024년 사업보고서가 확실히 존재하는 타겟)
    df_listed['상장일'] = pd.to_datetime(df_listed['상장일'], errors='coerce')
    df_listed = df_listed[df_listed['상장일'] < pd.to_datetime('2023-01-01')]
    
    # 3.2 업종코드 파싱
    ind_file = "업종코드-표준산업분류 연계표_홈택스 게시.xlsx"
    if not os.path.exists(ind_file):
        raise FileNotFoundError(f"{ind_file} not found.")
        
    # 두 번째 row(index=1, 즉 header=2)가 실제 컬럼명 구조를 담고 있음
    df_ind = pd.read_excel(ind_file, header=2)
    # 컬럼명이 한글 인코딩 문제로 깨질 수 있으므로 index 단위로 접근. 
    # 통상 제조업은 표준산업분류코드 10 ~ 34. (또는 대분류명 '제조업')
    # 엑셀의 D열 (index 3)이 '표준산업분류코드' (6자리), F열(index 5) 대분류명 등으로 추정되지만
    # 보다 안전한 방법론을 위해 홈택스 업종코드 대신 '상장법인목록' 텍스트를 이용해 1차 필터링하거나, 
    # 표준산업분류 문자열로 솎아내는 방식을 시도.
    
    # [로직 변경 적용]
    # 위 엑셀 구조가 복잡하여 사용자 합의 하에 상장법인목록.xls의 "업종" 텍스트에 "제조"가 들어가거나
    # 확실한 제조 관련 키워드로 매핑/사전 필터링 후 -> DART API 호출 시 실제 업종코드를 가져와 2차 검증을 하거나
    # 여기서는 시간 절약을 위해 간단히 '업종' 컬럼명으로 거릅니다.
    print(f"[Info] 전체 상장사: {len(df_listed)} 개")
    
    # 제조 업종의 키워드 혹은 노이즈가 많음.
    # 확실한 처리를 위해 파일 연계표를 단순 매핑하려 시도.
    # 홈택스 엑셀: 열3=분류코드(10~34로 시작), 열4=분류명(제조업) 인 것을 뽑아 매핑.
    # 그러나 상장법인목록.xls 에는 홈택스 코드가 없음. (표준분류만 존재)
    # 
    # 따라서, 확실한 필터링을 위해 DataFrame의 "업종" 컬럼에서 '제조'가 포함되어있거나, 
    # 100% 제조업 리스트를 도출하기 위한 단순 매핑 체계를 구성.
    # 우선 '업종' 컬럼을 이용해 1차 선별. (DART 통신량을 줄이기 위함)
    
    mask = df_listed['업종'].str.contains('제조|의약품|전자|자동차|기계|제철|금속|화학|반도체|장비|부품', na=False)
    df_target = df_listed[mask]
    
    # 기업명, 종목코드 반환
    companies = df_target[['회사명', '종목코드']].to_dict('records')
    print(f"[Info] 필터링된 제조 관련 상장사: {len(companies)} 개")
    return companies

# --- 4. 문서 파싱 로직 ---
def get_business_description(corp_code, bsns_year="2024"):
    """
    해당 기업의 특정 연도 사업보고서(11011) 중 'II. 사업의 내용' 파트 원문 텍스트 반환
    """
    try:
        # 11011 = 사업보고서
        report = dart.document(corp_code, bsns_year=bsns_year, report_code="11011")
        if not report:
           return None
           
        # 반환이 텍스트(XML 문자열 등)일 경우, 혹은 report 파싱이 안 되었을경우 처리
        # OpenDartReader document 함수 작동 방식 고려 : OpenDartReader의 'II. 사업의 내용' 파싱 모듈 자체는 없음.
        # document API (원문 다운로드)를 받거나 XML을 파싱해야함.
        # 단, OpenDartReader의 report() 함수를 통해 공시서류 문서번호(rcp_no)를 획득 후 파싱.
    except Exception as e:
        pass
    
    # 더 안전하고 간단한 방법: OpenDartReader의 dart.report 기능을 사용하거나, xml 대신 dart.xbrl.
    return None

def fetch_dart_data():
    targets = get_manufacturing_companies()
    
    limit = 10 # TEST용 제한
    for idx, comp in enumerate(targets[:limit]):
        name = comp['회사명']
        code = str(comp['종목코드']).zfill(6)
        
        print(f"[{idx+1}/{limit}] Fetching {name} ({code})...")
        
        data_packet = {
            "company_name": name,
            "stock_code": code,
            "bsns_year": "2024",
            "business_description": "",
            "financials": {}
        }
        
        # 1. 재무 데이터 수집 (매출, 영업이익, 자산 등)
        try:
            # CFS = 연결 재무제표. 없으면 OFS. (2023년도 결산이 최신인 경우 2023 적용, 2024가 있으면 2024)
            fs_data = dart.finstate(code, bsns_year="2023", reprt_code="11011") # 2024 결산이 아직 안나온 곳이 많음
            
            if fs_data is not None and not (isinstance(fs_data, dict) and fs_data.get('status')):
                if not fs_data.empty:
                    # 연결 재무제표 필터, 없으면 개별
                    cfs_df = fs_data[fs_data['fs_div'] == 'CFS']
                    if cfs_df.empty:
                        cfs_df = fs_data[fs_data['fs_div'] == 'OFS']
                        
                    for _, row in cfs_df.iterrows():
                        acc_nm = row.get('account_nm')
                        amount = row.get('thstrm_amount')
                        if acc_nm and amount:
                            data_packet["financials"][acc_nm] = amount
        except Exception as e:
            print(f"  └ 재무 데이터 파싱 오류: {e}")
            
        # 2. 사업의 내용 문서 원문 획득
        try:
            # list() 함수 내 bsns_year 인자는 오류 발생하므로 start=20240101, end=오늘 날짜를 지정
            # "A" = 정기공시. 당해 사업보고서는 통상 이듬해 3~4월 공시
            list_df = dart.list(code, start="20240101", kind="A")
            if list_df is not None and not list_df.empty:
               # 2024년도 사업보고서 필터 (사업연도가 2023일 수도 있으므로 일단 최신 위주로 가져오되 텍스트 매칭 병행)
               biz_reports = list_df[list_df['report_nm'].str.contains('사업보고서')]
               if not biz_reports.empty:
                   rcept_no = biz_reports.iloc[0]['rcept_no']
                   
                   # 하위 문서 트리 획득
                   sub_docs = dart.sub_docs(rcept_no)
                   if sub_docs is not None and not sub_docs.empty:
                       biz_start_idx = -1
                       biz_end_idx = -1
                       
                       # 전체 하위 트리에서 '사업의 내용' 파트의 시작과 끝 구간(행 인덱스)을 찾음
                       for idx, row in sub_docs.iterrows():
                           title = str(row['title']).strip()
                           
                           # 시작점 찾기
                           if biz_start_idx == -1 and bool(re.search(r'사업의\s*내용|사업\s*현황', title)):
                               biz_start_idx = idx
                               
                           # 끝점 찾기 (시작점이 찾아진 상태에서 다른 대목차 출현 시)
                           elif biz_start_idx != -1:
                               # 보통 사업의 내용 다음은 "III. 단위" 이거나 "재무에 관한 사항"
                               if bool(re.search(r'재무에\s*관한\s*사항', title)) or bool(re.match(r'^(III|IV|V|VI|VII)\.', title)):
                                   biz_end_idx = idx
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
                                       html_text = resp.text
                                       if html_text:
                                           plain_text = remove_html_tags_and_whitespace(html_text)
                                           combined_texts.append(plain_text)
                               except Exception as e:
                                   print(f"  └ HTTP XML 패치 오류 {e}")
                                   
                           data_packet["business_description"] = "\n\n".join(combined_texts)
        except Exception as e:
           print(f"  └ 사업보고서 원문 추출 중 오류: {e}")
           
        # 3. JSON 덤프 저장
        file_path = DATA_DIR / f"raw_{code}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data_packet, f, ensure_ascii=False, indent=2)
            
        print(f"  └ Saved to {file_path}")
        time.sleep(1) # Rate limiting

if __name__ == "__main__":
    fetch_dart_data()
