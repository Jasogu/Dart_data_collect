from pathlib import Path


def main():
    legacy = Path("data") / "stock_master_list.csv"
    if legacy.exists():
        legacy.unlink()
        print(f"Removed legacy file: {legacy}")

    print("create_master_table.py is deprecated.")
    print("This project now uses '상장법인목록.xls' D열(업종) directly in collect_dart_manufacturing.py.")


if __name__ == "__main__":
    main()
