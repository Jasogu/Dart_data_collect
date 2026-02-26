import os
import time
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import OpenDartReader

# 설정
load_dotenv()
DART_API_KEY = os.getenv("DART_API_KEY")
if not DART_API_KEY:
    print("Error: DART_API_KEY is not set in environment.")
    exit(1)

dart = OpenDartReader(DART_API_KEY)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
LISTED_FILE = "상장법인목록.xls"
IND_FILE = "업종코드-표준산업분류 연계표_홈택스 게시.xlsx"
OUTPUT_FILE = DATA_DIR / "stock_master_list.csv"

def create_master_table():
    print("1. 상장법인목록 파싱 중...")
    if not os.path.exists(LISTED_FILE):
        raise FileNotFoundError(f"{LISTED_FILE} not found.")
        
    try:
        df_listed = pd.read_html(LISTED_FILE, encoding='cp949')[0]
    except Exception as e:
        raise RuntimeError(f"Failed to parse {LISTED_FILE}. Ex: {e}")
        
    df_listed.columns = ['회사명', '소속부', '종목코드', '업종', '주요제품', '상장일', '결산월', '대표자명', '홈페이지', '지역']
    df_listed = df_listed[df_listed['소속부'].isin(['유가증권', '코스닥'])].copy()
    
    # 2023년 이전 상장 기업 필터링
    df_listed['상장일'] = pd.to_datetime(df_listed['상장일'], errors='coerce')
    df_listed = df_listed[df_listed['상장일'] < pd.to_datetime('2023-01-01')]
    df_listed['종목코드'] = df_listed['종목코드'].astype(str).str.zfill(6)
    
    print(f"  └ 필터링된 상장기업 수: {len(df_listed)} 개")

    print("2. 11차 표준산업분류 연계표 파싱 중...")
    IND_FILE_NEW = "업종코드-11차표준산업분류.xlsx"
    if not os.path.exists(IND_FILE_NEW):
        # 만약 새 파일 이름이 다를 경우 기존 파일로 폴백하되 에러 발생
        if os.path.exists(IND_FILE):
             IND_FILE_NEW = IND_FILE
        else:
             raise FileNotFoundError(f"{IND_FILE_NEW} not found.")
        
    # 새로운 11차 파일 구조 (테스트 기준):
    # row 0에 헤더 존재: 표준산업분류, 대분류명, 대분류코드, 중분류명, 중분류코드, 소분류명, 소분류코드, 세분류명, 세분류코드, 세세분류명 등
    df_ind = pd.read_excel(IND_FILE_NEW, header=1) # 0번 행이 헤더
    
    # 컬럼은 직접 인덱스로 추출 (0: 세세분류코드(또는 표준분류코드), 2: 대분류명, 4: 중분류명, 6: 소분류명, 8: 세분류명, 9: 세세분류명)
    # 실제 데이터 프레임의 헤더 구조 파악 (사용자 제공 예시 참고)
    # 0: 01110, 1: A, 2: 농업, ... 8: 곡물 재배업, 9: 곡물 및 기타 식량작물 재배업
    
    try:
        df_ind_core = df_ind.iloc[:, [0, 2, 4, 6, 8, 9]].copy()
        df_ind_core.columns = ['세세분류_코드', '대분류', '중분류', '소분류', '세분류_명', '세세분류_명']
    except Exception as e:
        print(f"Warning: 컬럼 인덱싱 실패. {e}")
        df_ind_core = df_ind.copy()
        
    df_ind_core = df_ind_core.dropna(subset=['세세분류_코드'])
    df_ind_core['세세분류_코드'] = df_ind_core['세세분류_코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    # 세세분류_코드가 5자리가 아닌 경우, 혹은 앞자리에 0이 빠진 경우(예: 1110 -> 01110) 패딩
    df_ind_core['세세분류_코드'] = df_ind_core['세세분류_코드'].apply(lambda x: x.zfill(5) if len(x) < 5 else x)
    
    # 세분류 코드(4자리) 도출 (세세분류 코드의 앞 4자리)
    df_ind_core['세분류_코드'] = df_ind_core['세세분류_코드'].str[:4]
    df_ind_core['세분류'] = df_ind_core['세분류_명'] # 이름 통일
    
    df_ind_core = df_ind_core.drop_duplicates(subset=['세세분류_코드'])

    print("3. '상장법인목록'의 업종명과 '11차 연계표'의 세세분류명/세분류명 텍스트 매핑 시작...")
    
    mapped_data = []
    
    total = len(df_listed)
    for idx, row in df_listed.iterrows():
        comp_name = row['회사명']
        stock_code = row['종목코드']
        market = row['소속부']
        dart_industry_name = row['업종'] # 텍스트 기반 매핑 대상
        
        # 기본값 세팅
        mapped_dict = {
            '회사명': comp_name,
            '종목코드': stock_code,
            '소속부': market,
            '표준산업분류코드': '미분류',
            '대분류': '미분류',
            '중분류': '미분류',
            '소분류': '미분류',
            '세분류': '미분류',
            '세세분류_코드': '미분류',
            '세세분류_명': dart_industry_name,
        }
        
        # --- 예외 처리 사전 (DART 업종명 -> 11차 표준산업분류명) ---
        # 기호나 명칭이 표준산업분류 원문과 아예 다른 경우
        exception_dict = {
            "석탄광업및채석업": "석탄광업",
            "동물용사료및조제식품제조업": "동물용사료및조제식품제조업",
            "기타비금속광물제품제조업": "기타비금속광물제품제조업",
            "종합소매업": "종합소매업",
            "농업소임업및어업": "농업임업및어업",
            "측정시험항해제어및기타정밀기기제조업": "측정시험항해제어및기타정밀기기제조업광학기기제외",
            "구조용금속제품탱크및증기발생기제조업": "구조용금속제품탱크및증기발생기제조업",
            "영화비디오물방송프로그램제작": "영화비디오물방송프로그램제작및배급업",
            "컴퓨터프로그래밍시스템통합": "컴퓨터프로그래밍시스템통합및관리업",
            "자료처리호스팅포털": "자료처리호스팅포털및기타인터넷정보매개서비스업",
            "서적잡지및기타인쇄물": "서적잡지및기타인쇄물출판업",
            "건축기술엔지니어링": "건축기술엔지니어링및관련기술서비스업",
            "도축육류가공및저장처리업": "도축육류가공및저장처리업",
            "시멘트석회플라스터": "시멘트석회플라스터및그제품제조업",
            "섬유의복신발및가죽제품소매업": "섬유의복신발및가죽제품소매업",
            "기타전문도매업": "기타전문도매업",
            "기계장비및관련물품도매업": "기계장비및관련물품도매업",
            "음식료품및담배도매업": "음식료품및담배도매업",
            "동식물성유지및낙농제품제조업": "동식물성유지및낙농제품제조업",
            "비료농약및살균살충제제조업": "비료농약및살균살충제제조업",
            "자동차차체나트레일러제조업": "자동차차체및트레일러제조업",
            "산업용농축산물및동식물도매업": "산업용농축산물및동식물도매업",
            "사업시설유지관리서비스업": "사업시설유지관리서비스업"
        }
        
        if pd.notna(dart_industry_name):
            target_name = str(dart_industry_name).strip()
            
            import re
            # 파이썬에서 \w는 'ㆍ'(가운뎃점, U+318D)을 단어 문자로 취급하므로 명시적으로 한글/영문/숫자만 남김
            target_name_no_space = re.sub(r'[^가-힣a-zA-Z0-9]', '', target_name)
            
            # 사전 예외 처리 적용 
            for key, val in exception_dict.items():
                 if key in target_name_no_space:
                      target_name_no_space = val
                      break
            
            # 매핑을 위해 연계표에도 동일한 엄격한 정규화 적용
            df_ind_core['match_세세분류명'] = df_ind_core['세세분류_명'].str.replace(r'[^가-힣a-zA-Z0-9]', '', regex=True)
            df_ind_core['match_세분류명'] = df_ind_core['세분류_명'].str.replace(r'[^가-힣a-zA-Z0-9]', '', regex=True)
            df_ind_core['match_소분류명'] = df_ind_core['소분류'].str.replace(r'[^가-힣a-zA-Z0-9]', '', regex=True)
            df_ind_core['match_중분류명'] = df_ind_core['중분류'].str.replace(r'[^가-힣a-zA-Z0-9]', '', regex=True)
            df_ind_core['match_대분류명'] = df_ind_core['대분류'].str.replace(r'[^가-힣a-zA-Z0-9]', '', regex=True)
            
            
            match_level = 0 # 1:세세, 2:세, 3:소, 4:중, 5:대
            
            # 1. '세세분류_명'과 완벽 일치 탐색
            match = df_ind_core[df_ind_core['match_세세분류명'] == target_name_no_space]
            if not match.empty: match_level = 1
            
            # 2. '세세분류_명'에 없으면 '세분류_명'에서 탐색
            if match.empty:
                 match = df_ind_core[df_ind_core['match_세분류명'] == target_name_no_space]
                 if not match.empty: match_level = 2
                 
            # 3. '소분류_명'에서 탐색
            if match.empty:
                 match = df_ind_core[df_ind_core['match_소분류명'] == target_name_no_space]
                 if not match.empty: match_level = 3
                 
            # 4. '중분류_명'에서 탐색
            if match.empty:
                 match = df_ind_core[df_ind_core['match_중분류명'] == target_name_no_space]
                 if not match.empty: match_level = 4
                 
            # 5. '대분류_명'에서 탐색
            if match.empty:
                 match = df_ind_core[df_ind_core['match_대분류명'] == target_name_no_space]
                 if not match.empty: match_level = 5
                 
            # 6. 그래도 없으면 부분 일치 탐색 (세세분류명 기준)
            if match.empty:
                match = df_ind_core[df_ind_core['match_세세분류명'].str.contains(target_name_no_space, na=False, regex=False)]
                if not match.empty: match_level = 1
            
            if not match.empty:
                info = match.iloc[0]
                mapped_dict['대분류'] = info['대분류']
                
                # 중분류 이상 매칭 시
                if match_level <= 4:
                     mapped_dict['중분류'] = info['중분류']
                
                # 소분류 이상 매칭 시
                if match_level <= 3:
                     mapped_dict['소분류'] = info['소분류']
                     
                # 세분류 이상 매칭 시
                if match_level <= 2:
                     mapped_dict['세분류'] = info['세분류_명']
                     
                # 세세분류 매칭 시
                if match_level == 1:
                     mapped_dict['세세분류_코드'] = info['세세분류_코드']
                     mapped_dict['표준산업분류코드'] = info['세세분류_코드']
                
                # 세세분류가 아닌 상위 계층으로 매핑된 경우, 코드는 해당 상위 계층의 코드를 사용할 수 없으므로(연계표 구조상) 기본값(미분류) 유지
        
        mapped_data.append(mapped_dict)

    master_df = pd.DataFrame(mapped_data)
    
    # 미분류율 체크
    unmapped = len(master_df[master_df['대분류'] == '미분류'])
    
    master_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"4. 마스터 테이블 생성 완료: {OUTPUT_FILE}")
    print(f"  └ 총 {len(master_df)}건 처리 완료. (매핑 실패/미분류: {unmapped}건)")

if __name__ == "__main__":
    create_master_table()
