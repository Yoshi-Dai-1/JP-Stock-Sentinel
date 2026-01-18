"""
週次でEDINETから定性情報を更新するスクリプト

このスクリプトは以下の処理を行います:
1. 週次チェック（最終実行から7日以上経過している場合のみ実行）
2. nikkei225_history.csvから全銘柄を読み込み
3. 各銘柄のEDINETデータを取得
4. data/{ticker}/info.jsonに保存
"""

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

# qualitative_extractorをインポート
from qualitative_extractor import (
    extract_from_edinet,
    should_run_weekly,
    update_last_run_date
)


def get_latest_report_date(fiscal_month=3):
    """
    最新の有価証券報告書の提出日を推定する
    
    Args:
        fiscal_month: 決算月（デフォルト: 3月）
        
    Returns:
        str: 推定される報告書提出日（YYYY-MM-DD形式）
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    # 有価証券報告書は決算日から3ヶ月以内に提出
    # 例: 3月決算 → 6月末までに提出
    report_month = (fiscal_month + 3) % 12
    if report_month == 0:
        report_month = 12
    
    # 今年の報告書提出月を過ぎているか確認
    if current_month > report_month:
        # 今年の報告書を取得
        report_year = current_year
    else:
        # 昨年の報告書を取得
        report_year = current_year - 1
    
    # 報告書提出日を推定（月末）
    if report_month in [1, 3, 5, 7, 8, 10, 12]:
        report_day = 31
    elif report_month in [4, 6, 9, 11]:
        report_day = 30
    else:  # 2月
        report_day = 28
    
    return f"{report_year}-{report_month:02d}-{report_day:02d}"


def update_edinet_data(force=False, limit=None):
    """
    全銘柄のEDINETデータを更新する
    
    Args:
        force: 週次チェックを無視して強制実行
        limit: 処理する銘柄数の上限（テスト用）
    """
    print("=" * 60)
    print("EDINET定性情報 週次更新")
    print("=" * 60)
    print()
    
    # 週次チェック
    if not should_run_weekly(force):
        print("週次チェックにより実行をスキップしました。")
        print("強制実行する場合は --force オプションを使用してください。")
        return
    
    # プロジェクトルートとデータディレクトリ
    project_root = Path(__file__).parent.parent
    master_file = project_root / "data" / "master" / "stocks_master.csv"
    
    if not master_file.exists():
        print(f"[エラー] {master_file} が見つかりません。")
        print("先に init_nikkei_list.py を実行してください。")
        return
    
    # 銘柄リストを読み込み
    print(f"銘柄リストを読み込み中: {master_file}")
    df = pd.read_csv(master_file)
    
    # 上場中の銘柄のみを対象 (is_active が True)
    active_stocks = df[df['is_active'] == True]
    
    total = len(active_stocks)
    if limit:
        active_stocks = active_stocks.head(limit)
        print(f"[テストモード] 最初の{limit}銘柄のみ処理します")
    
    print(f"対象銘柄数: {len(active_stocks)} / {total}")
    print()
    
    # APIキーを環境変数から取得
    api_key = os.getenv('EDINET_API_KEY')
    if not api_key:
        print("[警告] 環境変数 EDINET_API_KEY が設定されていません。")
        print("GitHub Secretsまたは環境変数に設定してください。")
        return
    
    # 各銘柄のデータを取得
    success_count = 0
    error_count = 0
    skip_count = 0
    
    for idx, row in active_stocks.iterrows():
        ticker = row['code']  # 'ticker' -> 'code' に変更
        # ティッカーから証券コードを抽出（例: 7203.T → 7203）
        sec_code = ticker.replace('.T', '')
        
        print(f"\n[{idx + 1}/{len(active_stocks)}] {ticker} ({row['name']})")
        
        # 最新の報告書提出日を推定
        target_date = get_latest_report_date()
        print(f"  対象日付: {target_date}")
        
        try:
            # EDINETからデータを取得
            results = extract_from_edinet(
                sec_code=sec_code,
                target_date=target_date,
                api_key=api_key,
                save_to_file=True
            )
            
            if results:
                success_count += 1
                print(f"  [成功] {len(results)}項目を抽出しました")
            else:
                skip_count += 1
                print(f"  [スキップ] データが見つかりませんでした")
            
            # APIレート制限を考慮して待機（1秒）
            time.sleep(1)
            
        except Exception as e:
            error_count += 1
            print(f"  [エラー] {e}")
            # エラー時も待機
            time.sleep(1)
    
    # 最終実行日を更新
    update_last_run_date()
    
    # 結果サマリー
    print()
    print("=" * 60)
    print("更新完了")
    print("=" * 60)
    print(f"成功: {success_count}")
    print(f"スキップ: {skip_count}")
    print(f"エラー: {error_count}")
    print(f"合計: {len(active_stocks)}")
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="EDINET定性情報を週次で更新")
    parser.add_argument(
        "--force",
        action="store_true",
        help="週次チェックを無視して強制実行"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="処理する銘柄数の上限（テスト用）"
    )
    
    args = parser.parse_args()
    
    update_edinet_data(force=args.force, limit=args.limit)
