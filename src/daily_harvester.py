"""
日次株価データ取得スクリプト (Daily Harvester)

yfinanceを使用して株価データを取得し、data/{ticker}/prices.csvに保存します。
1回の実行で最大100銘柄を処理し、進捗管理により中断・再開が可能です。
"""

import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import time
import random

# 定数定義
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MASTER_DIR = DATA_DIR / "master"
STOCKS_MASTER_FILE = MASTER_DIR / "stocks_master.csv"

# 設定
MAX_STOCKS_PER_RUN = 100  # 1回の実行で処理する最大銘柄数
MIN_SLEEP = 1.0  # 最小スリープ時間（秒）
MAX_SLEEP = 3.0  # 最大スリープ時間（秒）


def load_stocks_master():
    """stocks_master.csvを読み込む"""
    if not STOCKS_MASTER_FILE.exists():
        print(f"[エラー] {STOCKS_MASTER_FILE} が見つかりません。")
        sys.exit(1)
    
    df = pd.read_csv(STOCKS_MASTER_FILE)
    return df


def save_stocks_master(df):
    """stocks_master.csvを保存"""
    df.to_csv(STOCKS_MASTER_FILE, index=False, encoding='utf-8-sig')


def get_target_stocks(df, max_count=MAX_STOCKS_PER_RUN):
    """
    処理対象の銘柄を取得
    
    優先順位:
    1. last_price_updateが空（未取得）
    2. last_price_updateが古い順
    """
    # 上場中の銘柄のみ
    active_stocks = df[df['is_active'] == True].copy()
    
    # last_price_updateが空の場合は古い日付として扱う
    active_stocks['last_price_update_dt'] = pd.to_datetime(
        active_stocks['last_price_update'], 
        errors='coerce'
    ).fillna(pd.Timestamp('1900-01-01'))
    
    # 古い順にソート
    active_stocks = active_stocks.sort_values('last_price_update_dt')
    
    # 最大件数まで取得
    target_stocks = active_stocks.head(max_count)
    
    return target_stocks


def fetch_price_data(ticker_code, period='max'):
    """
    yfinanceで株価データを取得
    
    Args:
        ticker_code: ティッカーコード (例: 7203.T)
        period: 取得期間 ('max', '1y', '5y' など)
    
    Returns:
        pd.DataFrame: 株価データ (Date, Open, High, Low, Close, Volume)
    """
    try:
        ticker = yf.Ticker(ticker_code)
        # auto_adjust=True で分割調整済みの価格を取得
        hist = ticker.history(period=period, auto_adjust=True)
        
        if hist.empty:
            return None
        
        # インデックス（日付）をカラムに変換
        hist = hist.reset_index()
        
        # 必要なカラムのみ抽出
        columns_to_keep = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        hist = hist[[col for col in columns_to_keep if col in hist.columns]]
        
        return hist
    
    except Exception as e:
        print(f"  [エラー] データ取得失敗: {e}")
        return None


def save_price_data(ticker_code, price_data):
    """
    株価データをCSVに保存
    
    Args:
        ticker_code: ティッカーコード
        price_data: 株価データ (DataFrame)
    """
    ticker_dir = DATA_DIR / ticker_code
    ticker_dir.mkdir(parents=True, exist_ok=True)
    
    prices_file = ticker_dir / "prices.csv"
    
    # 既存データがあれば読み込んで結合
    if prices_file.exists():
        existing_data = pd.read_csv(prices_file, parse_dates=['Date'])
        
        # 新しいデータと結合（重複を削除）
        combined = pd.concat([existing_data, price_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=['Date'], keep='last')
        combined = combined.sort_values('Date')
        
        combined.to_csv(prices_file, index=False, encoding='utf-8-sig')
    else:
        price_data.to_csv(prices_file, index=False, encoding='utf-8-sig')


def update_last_price_update(df, ticker_code):
    """
    stocks_master.csvのlast_price_updateを更新
    
    Args:
        df: stocks_master DataFrame
        ticker_code: 更新する銘柄のティッカーコード
    """
    current_date = datetime.now().strftime('%Y-%m-%d')
    # 型互換性のため、明示的に文字列型として設定
    df['last_price_update'] = df['last_price_update'].astype(str)
    df.loc[df['code'] == ticker_code, 'last_price_update'] = current_date


def main():
    """メイン処理"""
    print("=" * 60)
    print("JP-Stock-Sentinel: 日次株価データ取得")
    print("=" * 60)
    print()
    
    # マスタデータ読み込み
    print("stocks_master.csv を読み込み中...")
    df = load_stocks_master()
    print(f"総銘柄数: {len(df)}")
    print()
    
    # 処理対象の銘柄を取得
    target_stocks = get_target_stocks(df, MAX_STOCKS_PER_RUN)
    print(f"処理対象銘柄数: {len(target_stocks)}")
    print()
    
    if len(target_stocks) == 0:
        print("処理対象の銘柄がありません。")
        return
    
    # 各銘柄のデータを取得
    success_count = 0
    error_count = 0
    
    for idx, row in target_stocks.iterrows():
        ticker_code = row['code']
        ticker_name = row['name']
        
        print(f"[{success_count + error_count + 1}/{len(target_stocks)}] {ticker_code} ({ticker_name})")
        
        # データ取得
        price_data = fetch_price_data(ticker_code)
        
        if price_data is not None and len(price_data) > 0:
            # データ保存
            save_price_data(ticker_code, price_data)
            
            # last_price_update更新
            update_last_price_update(df, ticker_code)
            
            success_count += 1
            print(f"  [成功] {len(price_data)}件のデータを保存しました")
        else:
            error_count += 1
            print(f"  [スキップ] データが取得できませんでした")
        
        # ランダムスリープ（レート制限対策）
        if idx < len(target_stocks) - 1:  # 最後の銘柄ではスリープしない
            sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
            time.sleep(sleep_time)
    
    # マスタデータを保存
    print()
    print("stocks_master.csv を更新中...")
    save_stocks_master(df)
    
    # 結果サマリー
    print()
    print("=" * 60)
    print("処理完了")
    print("=" * 60)
    print(f"成功: {success_count}")
    print(f"エラー: {error_count}")
    print(f"合計: {len(target_stocks)}")
    print()


if __name__ == "__main__":
    main()
