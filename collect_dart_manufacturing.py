import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import OpenDartReader
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Config
load_dotenv(dotenv_path=".env")
DART_API_KEY = os.getenv("DART_API_KEY")
if not DART_API_KEY:
    print("Error: DART_API_KEY is not set in environment.")
    raise SystemExit(1)

dart = OpenDartReader(DART_API_KEY)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

LISTED_FILE = "\uc0c1\uc7a5\ubc95\uc778\ubaa9\ub85d.xls"
STATUS_FILE = DATA_DIR / "collection_status.json"
MISSING_REPORT_LOG = Path("missing_2024_reports.log")
BUSINESS_FAIL_LOG = Path("business_content_extraction_failures.log")

INDUSTRY_KEY = "\uc18c\ubd84\ub958"
EXCLUDED_INDUSTRY_D = "\uae08\uc735 \uc9c0\uc6d0 \uc11c\ube44\uc2a4\uc5c5"

REPORT_PATTERN = r"\uc0ac\uc5c5\ubcf4\uace0\uc11c.*\b2024\b"
BIZ_START_PATTERN = r"\uc0ac\uc5c5\uc758\s*\ub0b4\uc6a9|\uc0ac\uc5c5\s*\ud604\ud669"
BIZ_END_PATTERN = r"\uc7ac\ubb34\uc5d0\s*\uad00\ud55c\s*\uc0ac\ud56d"

MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.5
REQUEST_TIMEOUT_SEC = 20
LOOP_SLEEP_SEC = 0.3


def remove_html_tags_and_whitespace(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "lxml")
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_stock_code(value) -> str:
    code = str(value).strip().replace("'", "").upper()
    if not code or code.lower() == "nan":
        return ""
    if len(code) < 6:
        code = code.zfill(6)
    return code


def clean_dir_name(name: str) -> str:
    if not name:
        return "UNKNOWN_INDUSTRY"
    return re.sub(r'[/\\?%*:|"<>]', "_", str(name).strip())


def append_log(path: Path, code: str, company: str, reason: str, detail: str = "") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts}\t{code}\t{company}\t{reason}\t{detail}\n")


def is_transient_error(exc: Exception) -> bool:
    name = type(exc).__name__
    transient_names = {
        "ConnectionError",
        "ConnectTimeout",
        "ReadTimeout",
        "Timeout",
        "ChunkedEncodingError",
        "SSLError",
    }
    if name in transient_names:
        return True
    msg = str(exc).lower()
    tokens = ["connection", "timed out", "timeout", "temporarily", "remote end closed"]
    return any(t in msg for t in tokens)


def load_status() -> dict:
    default = {
        "completed": [],
        "skipped_no_2024_report": [],
        "failed_business_content": [],
        "skipped_lookup_error": [],
    }
    if not STATUS_FILE.exists():
        return default

    with open(STATUS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    for k, v in default.items():
        if k not in data or not isinstance(data[k], list):
            data[k] = v
    return data


def save_status(status_data: dict) -> None:
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status_data, f, ensure_ascii=False, indent=2)


def load_targets_from_listed_file() -> pd.DataFrame:
    if not os.path.exists(LISTED_FILE):
        raise FileNotFoundError(f"{LISTED_FILE} not found.")

    # KIND xls is html-table based in practice.
    df = pd.read_html(LISTED_FILE, encoding="cp949")[0]
    if df.shape[1] < 4:
        raise RuntimeError(f"Unexpected column shape in {LISTED_FILE}: {df.shape}")

    col_company = df.columns[0]
    col_code = df.columns[2]
    col_industry = df.columns[3]

    out = pd.DataFrame(
        {
            "company_name": df[col_company].astype(str).str.strip(),
            "stock_code": df[col_code].apply(normalize_stock_code),
            "industry_d": df[col_industry].astype(str).str.strip(),
        }
    )
    out = out[out["stock_code"] != ""].copy()
    out = out[out["industry_d"] != EXCLUDED_INDUSTRY_D].copy()
    out = out.drop_duplicates(subset=["stock_code"], keep="first")
    return out


def get_corp_code(company_name: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            corp_code = dart.find_corp_code(company_name)
            if corp_code:
                return str(corp_code).strip(), ""
            return None, "corp_code_not_found"
        except Exception as exc:
            if is_transient_error(exc) and attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
                continue
            return None, f"find_corp_code_error:{type(exc).__name__}"


def dart_list_with_retry(identifier: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            list_df = dart.list(identifier, start="20240101", kind="A")
            return list_df, ""
        except Exception as exc:
            if is_transient_error(exc) and attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
                continue
            return None, f"dart_list_error:{type(exc).__name__}"


def find_2024_business_report(code: str, company_name: str):
    # 1) Primary query by stock code
    list_df, err = dart_list_with_retry(code)
    query_identifier = code

    # 2) Fallback for alphanumeric/unsupported stock code query
    if err.startswith("dart_list_error:ValueError") or (
        list_df is None and any(ch.isalpha() for ch in code)
    ):
        corp_code, corp_err = get_corp_code(company_name)
        if not corp_code:
            return None, f"lookup_error:{corp_err}", query_identifier
        query_identifier = corp_code
        list_df, err = dart_list_with_retry(corp_code)

    if err:
        return None, err, query_identifier
    if list_df is None or list_df.empty:
        return None, "no_disclosure_from_2024", query_identifier

    reports = list_df[list_df["report_nm"].astype(str).str.contains(REPORT_PATTERN, na=False, regex=True)]
    if reports.empty:
        return None, "no_2024_business_report", query_identifier

    return reports.iloc[0], "", query_identifier


def extract_business_description(rcept_no: str):
    sub_docs = None
    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sub_docs = dart.sub_docs(rcept_no)
            last_err = ""
            break
        except Exception as exc:
            last_err = f"sub_docs_error:{type(exc).__name__}"
            if is_transient_error(exc) and attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
                continue
            break

    if last_err:
        return "", last_err
    if sub_docs is None or sub_docs.empty:
        return "", "sub_docs_empty"

    biz_start_idx = -1
    biz_end_idx = -1
    for i in range(len(sub_docs)):
        title = str(sub_docs.iloc[i].get("title", "")).strip()
        if biz_start_idx == -1 and re.search(BIZ_START_PATTERN, title):
            biz_start_idx = i
            continue
        if biz_start_idx != -1:
            if re.search(BIZ_END_PATTERN, title) or re.match(r"^(III|IV|V|VI|VII)\.", title):
                biz_end_idx = i
                break

    if biz_start_idx == -1:
        return "", "business_section_not_found"
    if biz_end_idx == -1:
        biz_end_idx = len(sub_docs)

    combined_texts = []
    fetch_errors = 0

    for i in range(biz_start_idx, biz_end_idx):
        doc_url = str(sub_docs.iloc[i].get("url", "")).strip()
        if not doc_url:
            continue

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(doc_url, timeout=REQUEST_TIMEOUT_SEC)
                if resp.status_code != 200:
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_BACKOFF_SEC * attempt)
                        continue
                    fetch_errors += 1
                    break
                cleaned = remove_html_tags_and_whitespace(resp.text)
                if cleaned:
                    combined_texts.append(cleaned)
                success = True
                break
            except Exception as exc:
                if is_transient_error(exc) and attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SEC * attempt)
                    continue
                fetch_errors += 1
                break
        if not success:
            continue

    if not combined_texts:
        if fetch_errors > 0:
            return "", "business_docs_fetch_failed_or_empty"
        return "", "business_docs_empty"

    business_description = "\n\n".join(combined_texts).strip()
    if not business_description:
        return "", "business_description_empty_after_cleaning"
    return business_description, ""


def fetch_financials_2023(identifier: str) -> dict:
    out = {}
    try:
        fs_data = dart.finstate(identifier, bsns_year="2023", reprt_code="11011")
        if fs_data is None:
            return out
        if isinstance(fs_data, dict) and fs_data.get("status"):
            return out
        if fs_data.empty:
            return out

        cfs_df = fs_data[fs_data["fs_div"] == "CFS"]
        if cfs_df.empty:
            cfs_df = fs_data[fs_data["fs_div"] == "OFS"]

        for _, fs_row in cfs_df.iterrows():
            acc_nm = fs_row.get("account_nm")
            amount = fs_row.get("thstrm_amount")
            if acc_nm and amount:
                out[acc_nm] = amount
    except Exception:
        pass
    return out


def fetch_dart_data() -> None:
    df_targets = load_targets_from_listed_file()
    status = load_status()

    completed = set(status.get("completed", []))
    skipped_no_report = set(status.get("skipped_no_2024_report", []))
    failed_biz = set(status.get("failed_business_content", []))
    skipped_lookup_error = set(status.get("skipped_lookup_error", []))

    # Retry business-content failures on every run (often transient network issues).
    processed = completed | skipped_no_report | skipped_lookup_error

    print(f"[Info] Total targets from listed file: {len(df_targets)}")
    print(f"[Info] Excluded industry (D): {EXCLUDED_INDUSTRY_D}")
    print(f"[Info] Already processed (completed/no_report/lookup_error): {len(processed)}")
    print(f"[Info] Remaining to process now: {len(df_targets) - len(processed)}")

    success_count = 0
    no_report_count = 0
    biz_fail_count = 0
    lookup_error_count = 0

    for idx, row in df_targets.iterrows():
        comp_name = str(row.get("company_name", "")).strip()
        code = normalize_stock_code(row.get("stock_code", ""))
        d_industry = str(row.get("industry_d", "")).strip()

        if not code:
            continue
        if code in processed:
            continue

        print(f"[{idx + 1}/{len(df_targets)}] Checking {comp_name} ({code})")

        report_row, report_reason, query_identifier = find_2024_business_report(code, comp_name)
        if report_row is None:
            if report_reason.startswith("lookup_error:") or report_reason.startswith("dart_list_error:"):
                lookup_error_count += 1
                skipped_lookup_error.add(code)
                status["skipped_lookup_error"] = sorted(skipped_lookup_error)
                save_status(status)
                append_log(MISSING_REPORT_LOG, code, comp_name, report_reason, "cannot_query_dart")
                print(f"  └ [Skip] Lookup error: {report_reason}")
                time.sleep(LOOP_SLEEP_SEC)
                continue

            no_report_count += 1
            skipped_no_report.add(code)
            status["skipped_no_2024_report"] = sorted(skipped_no_report)
            save_status(status)
            append_log(MISSING_REPORT_LOG, code, comp_name, report_reason, "")
            print("  └ [Skip] No 2024 business report")
            time.sleep(LOOP_SLEEP_SEC)
            continue

        rcept_no = str(report_row.get("rcept_no", "")).strip()
        business_description, biz_reason = extract_business_description(rcept_no)
        if not business_description:
            biz_fail_count += 1
            failed_biz.add(code)
            status["failed_business_content"] = sorted(failed_biz)
            save_status(status)
            append_log(BUSINESS_FAIL_LOG, code, comp_name, biz_reason, f"rcept_no={rcept_no}")
            print(f"  └ [Skip] Business content extraction failed: {biz_reason}")
            time.sleep(LOOP_SLEEP_SEC)
            continue

        financials = fetch_financials_2023(query_identifier)
        industry_folder_name = clean_dir_name(d_industry)
        target_dir = DATA_DIR / industry_folder_name
        target_dir.mkdir(parents=True, exist_ok=True)

        data_packet = {
            "company_name": comp_name,
            "stock_code": code,
            "bsns_year": "2024",
            "industry_classification": {
                INDUSTRY_KEY: d_industry,
            },
            "business_description": business_description,
            "financials": financials,
        }

        file_path = target_dir / f"raw_{code}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data_packet, f, ensure_ascii=False, indent=2)

        completed.add(code)
        if code in failed_biz:
            failed_biz.discard(code)
            status["failed_business_content"] = sorted(failed_biz)
        status["completed"] = sorted(completed)
        save_status(status)

        success_count += 1
        print(f"  └ [Saved] {file_path}")
        time.sleep(LOOP_SLEEP_SEC)

    print("\n[Done] Collection run finished")
    print(f"  - saved_json: {success_count}")
    print(f"  - skipped_no_2024_report: {no_report_count}")
    print(f"  - skipped_business_content_fail: {biz_fail_count}")
    print(f"  - skipped_lookup_error: {lookup_error_count}")


if __name__ == "__main__":
    fetch_dart_data()
