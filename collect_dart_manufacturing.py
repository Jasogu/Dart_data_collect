import json
import os
import re
import time
from pathlib import Path

import OpenDartReader
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# --- 1. 설정 및 초기화 ---
load_dotenv()
DART_API_KEY = os.getenv("DART_API_KEY")
if not DART_API_KEY:
    print("Error: DART_API_KEY is not set in environment.")
    exit(1)

dart = OpenDartReader(DART_API_KEY)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

LISTED_FILE = "상장법인목록.xls"
STATUS_FILE = DATA_DIR / "collection_status.json"


def remove_html_tags_and_whitespace(html_text: str) -> str:
    """html 텍스트에서 태그를 모두 제거하고 평문으로 변환"""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "lxml")
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_status():
    if STATUS_FILE.exists():
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed": []}


def save_status(status_data):
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2)


def clean_dir_name(name: str) -> str:
    if not name:
        return "기타"
    return re.sub(r'[/\\?%*:|"<>]', "_", str(name).strip())


def load_targets_from_listed_file() -> pd.DataFrame:
    """상장법인목록.xls에서 직접 타겟(회사명/종목코드/업종 D열)을 구성."""
    if not os.path.exists(LISTED_FILE):
        raise FileNotFoundError(f"{LISTED_FILE} not found.")

    df = pd.read_html(LISTED_FILE, encoding="cp949")[0]
    if df.shape[1] < 6:
        raise RuntimeError("상장법인목록.xls 컬럼 구조를 확인해 주세요.")

    # 원본 컬럼 인덱스 기반 사용:
    # 0=회사명, 1=시장구분, 2=종목코드, 3=업종(D열), 5=상장일
    col_company = df.columns[0]
    col_market = df.columns[1]
    col_code = df.columns[2]
    col_industry = df.columns[3]
    col_listed_date = df.columns[5]

    df = df[df[col_market].isin(["유가증권", "코스닥"])].copy()
    df[col_listed_date] = pd.to_datetime(df[col_listed_date], errors="coerce")
    df = df[df[col_listed_date] < pd.to_datetime("2023-01-01")]

    out = pd.DataFrame(
        {
            "회사명": df[col_company].astype(str).str.strip(),
            "종목코드": df[col_code].astype(str).str.zfill(6),
            "소분류_D열": df[col_industry].astype(str).str.strip(),
        }
    )
    out = out[out["종목코드"].str.fullmatch(r"\d{6}", na=False)].copy()
    out = out.drop_duplicates(subset=["종목코드"], keep="first")
    return out


def fetch_dart_data():
    df_targets = load_targets_from_listed_file()
    status = load_status()
    completed_codes = set(status.get("completed", []))

    print(f"[Info] 전체 타겟 상장사: {len(df_targets)} 개")
    print(f"[Info] 이미 수집 완료된 상장사: {len(completed_codes)} 개")

    for idx, row in df_targets.iterrows():
        comp_name = str(row.get("회사명", "")).strip()
        code = str(row.get("종목코드", "")).zfill(6)
        d_industry = str(row.get("소분류_D열", "")).strip()

        if not code or code == "000nan":
            continue
        if code in completed_codes:
            continue

        industry_folder_name = clean_dir_name(d_industry)
        target_dir = DATA_DIR / industry_folder_name
        target_dir.mkdir(parents=True, exist_ok=True)

        print(f"[{idx + 1}/{len(df_targets)}] Fetching {comp_name} ({code})... -> {industry_folder_name}")

        data_packet = {
            "company_name": comp_name,
            "stock_code": code,
            "bsns_year": "2024",
            "industry_classification": {
                "소분류": d_industry,
            },
            "business_description": "",
            "financials": {},
        }

        # 1) 재무 데이터 수집
        try:
            fs_data = dart.finstate(code, bsns_year="2023", reprt_code="11011")
            if fs_data is not None and not (isinstance(fs_data, dict) and fs_data.get("status")):
                if not fs_data.empty:
                    cfs_df = fs_data[fs_data["fs_div"] == "CFS"]
                    if cfs_df.empty:
                        cfs_df = fs_data[fs_data["fs_div"] == "OFS"]
                    for _, fs_row in cfs_df.iterrows():
                        acc_nm = fs_row.get("account_nm")
                        amount = fs_row.get("thstrm_amount")
                        if acc_nm and amount:
                            data_packet["financials"][acc_nm] = amount
        except Exception:
            pass

        # 2) 사업의 내용 텍스트 수집
        has_2024_report = False
        try:
            list_df = dart.list(code, start="20240101", kind="A")
            if list_df is not None and not list_df.empty:
                biz_reports = list_df[list_df["report_nm"].str.contains(r"사업보고서.*\b2024\b")]
                if not biz_reports.empty:
                    has_2024_report = True
                    rcept_no = biz_reports.iloc[0]["rcept_no"]
                    sub_docs = dart.sub_docs(rcept_no)

                    if sub_docs is not None and not sub_docs.empty:
                        biz_start_idx, biz_end_idx = -1, -1
                        for i, sub_row in sub_docs.iterrows():
                            title = str(sub_row["title"]).strip()
                            if biz_start_idx == -1 and bool(re.search(r"사업의\s*내용|사업\s*현황", title)):
                                biz_start_idx = i
                            elif biz_start_idx != -1:
                                if bool(re.search(r"재무에\s*관한\s*사항", title)) or bool(
                                    re.match(r"^(III|IV|V|VI|VII)\.", title)
                                ):
                                    biz_end_idx = i
                                    break

                        if biz_start_idx != -1:
                            if biz_end_idx == -1:
                                biz_end_idx = len(sub_docs)
                            combined_texts = []
                            for i in range(biz_start_idx, biz_end_idx):
                                doc_url = sub_docs.iloc[i]["url"]
                                try:
                                    resp = requests.get(doc_url, timeout=20)
                                    if resp.status_code == 200:
                                        combined_texts.append(remove_html_tags_and_whitespace(resp.text))
                                except Exception:
                                    pass
                            data_packet["business_description"] = "\n\n".join(combined_texts)
        except Exception:
            pass

        if not has_2024_report:
            print(f"  └ [Warning] {comp_name} ({code}): 2024년 사업보고서(2024 기재)를 찾을 수 없습니다.")
            with open("missing_2024_reports.log", "a", encoding="utf-8") as log_f:
                log_f.write(f"{code},{comp_name},2024년 사업보고서 미제출(또는 검색 실패)\n")

        # 3) 저장 + 상태 업데이트
        file_path = target_dir / f"raw_{code}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_packet, f, ensure_ascii=False, indent=2)

        completed_codes.add(code)
        status["completed"] = list(completed_codes)
        save_status(status)
        time.sleep(1)


if __name__ == "__main__":
    fetch_dart_data()
