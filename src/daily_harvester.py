"""
日次データ総合取得スクリプト (Daily Harvester)

このスクリプトは以下の処理を行います:
1. stocks_master.csvから取得対象銘柄を特定
2. yfinanceで株価と四半期決算データを取得（差分のみ）
3. qualitative_extractorでEDINETから定性情報を取得

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

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# qualitative_extractorをインポート
try:
    from qualitative_extractor import extract_from_edinet
except ImportError:
    print("[警告] qualitative_extractor.py が見つかりません。定性情報の取得はスキップされます。")
    extract_from_edinet = None

# 定数定義
DATA_DIR = PROJECT_ROOT / "data"
MASTER_DIR = DATA_DIR / "master"
STOCKS_MASTER_FILE = MASTER_DIR / "stocks_master.csv"

# 設定
MAX_STOCKS_PER_RUN = 100
MIN_SLEEP = 1.0
MAX_SLEEP = 3.0


def load_stocks_master():
    """stocks_master.csvを読み込む"""
    if not STOCKS_MASTER_FILE.exists():
        print(f"[エラー] {STOCKS_MASTER_FILE} が見つかりません。")
        sys.exit(1)
    
    df = pd.read_csv(STOCKS_MASTER_FILE)
    # 型を明示的に設定
    df['last_price_update'] = df['last_price_update'].astype(str)
    df['last_info_update'] = df['last_info_update'].astype(str)
    return df


def save_stocks_master(df):
    """stocks_master.csvを保存"""
    df.to_csv(STOCKS_MASTER_FILE, index=False, encoding='utf-8-sig')


def get_target_stocks(df, max_count=MAX_STOCKS_PER_RUN):
    """処理対象の銘柄を取得（古い順）"""
    active_stocks = df[df['is_active'] == True].copy()
    
    # last_price_updateが空の場合は古い日付として扱う
    active_stocks['last_price_update_dt'] = pd.to_datetime(
        active_stocks['last_price_update'], 
        errors='coerce'
    ).fillna(pd.Timestamp('1900-01-01'))
    
    # 古い順にソート
    active_stocks = active_stocks.sort_values('last_price_update_dt')
    target_stocks = active_stocks.head(max_count)
    
    return target_stocks


def get_last_date_in_csv(csv_file):
    """CSVファイルから最終日付を取得"""
    if not csv_file.exists():
        return None
    
    try:
        df = pd.read_csv(csv_file, parse_dates=['Date'])
        if len(df) > 0:
            return df['Date'].max()
    except Exception:
        pass
    
    return None


def check_stock_actions(ticker_code, last_date):
    """
    株式分割・併合などのイベントを確認
    
    Args:
        ticker_code: ティッカーコード
        last_date: 既存データの最終日付
    
    Returns:
        bool: 新しいイベントがあればTrue
    """
    if last_date is None:
        return False  # 初回取得時はチェック不要
    
    try:
        ticker = yf.Ticker(ticker_code)
        actions = ticker.actions
        
        if actions is None or actions.empty:
            return False
        
        # 最終日以降のイベントを確認
        actions = actions.reset_index()
        actions['Date'] = pd.to_datetime(actions['Date'])
        
        # Stock Splits（株式分割）列が存在するか確認
        if 'Stock Splits' in actions.columns:
            recent_splits = actions[actions['Date'] > last_date]
            recent_splits = recent_splits[recent_splits['Stock Splits'] != 0]
            
            if len(recent_splits) > 0:
                print(f"  [検知] 株式分割・併合イベント: {len(recent_splits)}件")
                return True
        
        return False
    
    except Exception as e:
        print(f"  [警告] イベント確認エラー: {e}")
        return False


def fetch_price_data_incremental(ticker_code, last_date=None, force_full=False):
    """
    株価データを差分取得（分割・併合時は全期間取得）
    
    Args:
        ticker_code: ティッカーコード
        last_date: 既存データの最終日付（Noneの場合は全件取得）
        force_full: Trueの場合は強制的に全期間取得
    
    Returns:
        pd.DataFrame: 株価データ
    """
    try:
        ticker = yf.Ticker(ticker_code)
        
        # 取得期間を決定
        if last_date is None or force_full:
            # 初回または強制全期間取得
            hist = ticker.history(period='max', auto_adjust=True)
        else:
            # 差分のみ取得（最終日の翌日から）
            start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
            hist = ticker.history(start=start_date, auto_adjust=True)
        
        if hist.empty:
            return None
        
        # インデックス（日付）をカラムに変換
        hist = hist.reset_index()
        
        # 必要なカラムのみ抽出
        columns_to_keep = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        hist = hist[[col for col in columns_to_keep if col in hist.columns]]
        
        return hist
    
    except Exception as e:
        print(f"  [エラー] 株価データ取得失敗: {e}")
        return None


def fetch_quarterly_financials(ticker_code):
    """
    四半期決算データを取得
    
    Args:
        ticker_code: ティッカーコード
    
    Returns:
        pd.DataFrame: 四半期決算データ
    """
    try:
        ticker = yf.Ticker(ticker_code)
        quarterly = ticker.quarterly_financials
        
        if quarterly is None or quarterly.empty:
            print(f"  [情報] {ticker_code}: 財務データなし")
            return None
        
        # 転置して日付を行に
        quarterly = quarterly.T
        
        # インデックス（日付）に名前がない場合やDateでない場合に対応
        quarterly = quarterly.reset_index()
        quarterly = quarterly.rename(columns={'index': 'Date'})
        
        # 全ての項目がNaNの列（決算期）があれば削除するが、
        # 一部の項目でも値があれば保持する
        quarterly = quarterly.dropna(how='all', subset=[c for c in quarterly.columns if c != 'Date'])
        
        if quarterly.empty:
            print(f"  [情報] {ticker_code}: 有効な財務項目なし")
            return None
            
        return quarterly
    
    except Exception as e:
        print(f"  [警告] 決算データ取得失敗 ({ticker_code}): {e}")
        return None


def save_price_data(ticker_code, price_data, overwrite=False):
    """
    株価データをCSVに保存（追記モードまたは上書きモード）
    
    Args:
        ticker_code: ティッカーコード
        price_data: 株価データ
        overwrite: Trueの場合は上書き保存
    """
    ticker_dir = DATA_DIR / ticker_code
    ticker_dir.mkdir(parents=True, exist_ok=True)
    
    prices_file = ticker_dir / "prices.csv"
    
    if overwrite or not prices_file.exists():
        # 上書き保存または新規作成
        price_data.to_csv(prices_file, index=False, encoding='utf-8-sig')
    else:
        # 既存データと結合
        existing_data = pd.read_csv(prices_file, parse_dates=['Date'])
        combined = pd.concat([existing_data, price_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=['Date'], keep='last')
        combined = combined.sort_values('Date')
        combined.to_csv(prices_file, index=False, encoding='utf-8-sig')


def save_financials_data(ticker_code, financials_data):
    """
    決算データをCSVに保存（マージ追記）
    
    Args:
        ticker_code: ティッカーコード
        financials_data: 四半期決算データ
    """
    ticker_dir = DATA_DIR / ticker_code
    ticker_dir.mkdir(parents=True, exist_ok=True)
    
    financials_file = ticker_dir / "financials.csv"
    
    if financials_file.exists():
        try:
            # 既存データを読み込み
            existing_df = pd.read_csv(financials_file)
            
            # 新旧データを結合
            combined = pd.concat([existing_df, financials_data], ignore_index=True)
            
            # 日付カラムで重複を削除（新しい方を残す）
            if 'Date' in combined.columns:
                # yfから来るDateは文字列の場合もあればTimestampの場合もあるため統一
                combined['Date'] = pd.to_datetime(combined['Date'])
                combined = combined.drop_duplicates(subset=['Date'], keep='last')
                combined = combined.sort_values('Date', ascending=False) # 最新を上に
                
                # 文字列に戻して保存
                combined['Date'] = combined['Date'].dt.strftime('%Y-%m-%d')
            
            combined.to_csv(financials_file, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"  [警告] 決算データマージ失敗: {e}")
            financials_data.to_csv(financials_file, index=False, encoding='utf-8-sig')
    else:
        # 新規作成
        financials_data.to_csv(financials_file, index=False, encoding='utf-8-sig')


def fetch_qualitative_info(ticker_code, api_key):
    """
    EDINETから定性情報を取得
    
    Args:
        ticker_code: ティッカーコード
        api_key: EDINET APIキー
    
    Returns:
        bool: 取得成功したかどうか
    """
    if extract_from_edinet is None:
        return False
    
    try:
        # ティッカーから証券コードを抽出（例: 7203.T → 7203）
        sec_code = ticker_code.replace('.T', '')
        
        # 最新の報告書提出日を推定（3月決算の場合、6月末）
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        # 簡易的に前年度の報告書を取得
        if current_month >= 6:
            target_date = f"{current_year}-06-30"
        else:
            target_date = f"{current_year - 1}-06-30"
        
        # EDINETから取得
        results = extract_from_edinet(
            sec_code=sec_code,
            target_date=target_date,
            api_key=api_key,
            save_to_file=True
        )
        
        return results is not None and len(results) > 0
    
    except Exception as e:
        print(f"  [エラー] 定性情報取得失敗: {e}")
        return False


def update_last_update(df, ticker_code, update_type='price'):
    """
    stocks_master.csvの最終更新日時を更新
    
    Args:
        df: stocks_master DataFrame
        ticker_code: 更新する銘柄のティッカーコード
        update_type: 'price' または 'info'
    """
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    if update_type == 'price':
        df.loc[df['code'] == ticker_code, 'last_price_update'] = current_date
    elif update_type == 'info':
        df.loc[df['code'] == ticker_code, 'last_info_update'] = current_date


def main():
    """メイン処理"""
    print("=" * 60)
    print("JP-Stock-Sentinel: 日次データ総合取得")
    print("=" * 60)
    print()
    
    # EDINET APIキーを環境変数から取得
    edinet_api_key = os.getenv('EDINET_API_KEY')
    if edinet_api_key:
        print("[OK] EDINET_API_KEY が設定されています")
    else:
        print("[警告] EDINET_API_KEY が設定されていません。定性情報の取得はスキップされます。")
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
    price_success = 0
    financials_success = 0
    info_success = 0
    error_count = 0
    
    for idx, row in target_stocks.iterrows():
        ticker_code = row['code']
        ticker_name = row['name']
        
        print(f"[{price_success + error_count + 1}/{len(target_stocks)}] {ticker_code} ({ticker_name})")
        
        # 1. 株価データ取得（差分または全期間）
        ticker_dir = DATA_DIR / ticker_code
        prices_file = ticker_dir / "prices.csv"
        last_date = get_last_date_in_csv(prices_file)
        
        # 株式分割・併合のチェック
        has_stock_action = check_stock_actions(ticker_code, last_date)
        
        if has_stock_action:
            print(f"  株式分割・併合を検知 → 全期間データで上書き")
            price_data = fetch_price_data_incremental(ticker_code, last_date, force_full=True)
            should_overwrite = True
        elif last_date:
            print(f"  最終データ: {last_date.strftime('%Y-%m-%d')} → 差分取得")
            price_data = fetch_price_data_incremental(ticker_code, last_date, force_full=False)
            should_overwrite = False
        else:
            print(f"  初回取得 → 全件取得")
            price_data = fetch_price_data_incremental(ticker_code, None, force_full=False)
            should_overwrite = False
        
        if price_data is not None and len(price_data) > 0:
            save_price_data(ticker_code, price_data, overwrite=should_overwrite)
            update_last_update(df, ticker_code, 'price')
            price_success += 1
            
            if should_overwrite:
                print(f"  [株価] {len(price_data)}件のデータで上書き保存")
            else:
                print(f"  [株価] {len(price_data)}件のデータを追記")
        else:
            print(f"  [株価] 新しいデータなし")
        
        # 2. 四半期決算データ取得
        financials_data = fetch_quarterly_financials(ticker_code)
        if financials_data is not None and len(financials_data) > 0:
            save_financials_data(ticker_code, financials_data)
            financials_success += 1
            print(f"  [決算] {len(financials_data)}件のデータを保存")
        
        # 3. 定性情報取得（EDINET）
        if edinet_api_key and extract_from_edinet:
            if fetch_qualitative_info(ticker_code, edinet_api_key):
                update_last_update(df, ticker_code, 'info')
                info_success += 1
                print(f"  [定性] 取得成功")
        
        # ランダムスリープ
        if idx < len(target_stocks) - 1:
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
    print(f"株価データ取得: {price_success}")
    print(f"決算データ取得: {financials_success}")
    print(f"定性情報取得: {info_success}")
    print(f"合計処理: {len(target_stocks)}")
    print()


if __name__ == "__main__":
    main()
