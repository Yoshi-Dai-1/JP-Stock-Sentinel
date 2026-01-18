"""
日経225公式CSVから銘柄リストを取得し、新しいマスタ構造で保存するスクリプト

このスクリプトは以下の処理を行います:
1. 日経公式サイトからCSVをダウンロード
2. stocks_master.csv に銘柄基本情報を保存
3. index_history.csv に指数採用履歴をイベント形式で記録
4. 各銘柄用のディレクトリを作成
"""

import os
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path


# 定数定義
NIKKEI_CSV_URL = "https://indexes.nikkei.co.jp/nkave/archives/file/nikkei_stock_average_weight_jp.csv"
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MASTER_DIR = DATA_DIR / "master"
STOCKS_MASTER_FILE = MASTER_DIR / "stocks_master.csv"
INDEX_HISTORY_FILE = MASTER_DIR / "index_history.csv"


def download_nikkei_csv():
    """
    日経公式サイトからCSVをダウンロードし、DataFrameとして返す
    
    Returns:
        pd.DataFrame: 日経225銘柄のデータ
    """
    print("日経225公式CSVをダウンロード中...")
    response = requests.get(NIKKEI_CSV_URL)
    response.raise_for_status()
    
    # Shift_JISでデコードしてDataFrameに変換
    from io import StringIO
    csv_content = response.content.decode('shift_jis')
    df = pd.read_csv(StringIO(csv_content))
    
    print(f"[OK] {len(df)}銘柄のデータを取得しました")
    return df


def clean_and_convert_data(df):
    """
    データをクリーニングし、ティッカー形式に変換
    
    Args:
        df (pd.DataFrame): 日経225銘柄のデータ
        
    Returns:
        pd.DataFrame: クリーニング済みのデータ
    """
    print("データをクリーニング中...")
    
    # データのクリーニング: コード列が数値に変換できる行のみを保持
    df = df.copy()
    df = df[pd.to_numeric(df['コード'], errors='coerce').notna()]
    
    # ティッカー形式に変換（例: 7203 → 7203.T）
    df['code'] = df['コード'].astype(float).astype(int).astype(str) + '.T'
    
    # 銘柄名の列名を統一
    if '銘柄名' in df.columns:
        df['name'] = df['銘柄名']
    elif '名称' in df.columns:
        df['name'] = df['名称']
    else:
        # 最初の文字列型の列を銘柄名として使用
        for col in df.columns:
            if df[col].dtype == 'object' and col != 'コード':
                df['name'] = df[col]
                break
    
    # セクター情報を抽出（存在する場合）
    if '業種' in df.columns:
        df['sector'] = df['業種']
    elif '36業種' in df.columns:
        df['sector'] = df['36業種']
    else:
        df['sector'] = ''
    
    # さらにname列のクリーニング
    df = df[df['name'].notna()]
    df = df[~df['name'].str.contains('著作物|複写|複製|転載', na=False)]
    
    print(f"[OK] {len(df)}銘柄のデータをクリーニングしました")
    return df


def create_or_update_stocks_master(df):
    """
    stocks_master.csv を作成または更新
    
    Args:
        df (pd.DataFrame): クリーニング済みの銘柄データ
    """
    print("stocks_master.csv を作成中...")
    
    # ディレクトリが存在しない場合は作成
    MASTER_DIR.mkdir(parents=True, exist_ok=True)
    
    # 新しい銘柄データを作成
    new_stocks = pd.DataFrame({
        'code': df['code'],
        'name': df['name'],
        'sector': df['sector'],
        'is_active': True
    })
    
    # 既存のstocks_master.csvがあれば読み込み
    if STOCKS_MASTER_FILE.exists():
        existing_stocks = pd.read_csv(STOCKS_MASTER_FILE)
        
        # 新しい銘柄を追加（既存銘柄は更新）
        # codeをキーにしてマージ
        merged = pd.merge(
            existing_stocks[['code', 'is_active']],
            new_stocks,
            on='code',
            how='outer',
            suffixes=('_old', '_new')
        )
        
        # is_activeの更新: 新しいリストにあればTrue、なければ既存の値を保持
        merged['is_active'] = merged['is_active_new'].fillna(merged['is_active_old']).fillna(False)
        merged['name'] = merged['name_new'].fillna(merged['name_old'])
        merged['sector'] = merged['sector_new'].fillna(merged['sector_old']).fillna('')
        
        # 必要な列のみを選択
        stocks_master = merged[['code', 'name', 'sector', 'is_active']]
    else:
        stocks_master = new_stocks
    
    # CSVとして保存（UTF-8 with BOM for Excel compatibility）
    stocks_master.to_csv(STOCKS_MASTER_FILE, index=False, encoding='utf-8-sig')
    
    print(f"[OK] stocks_master.csv を作成しました: {STOCKS_MASTER_FILE}")
    print(f"  - 銘柄数: {len(stocks_master)}")
    
    return stocks_master


def create_or_update_index_history(df, index_name='Nikkei225'):
    """
    index_history.csv を作成または更新（差分のみ追記）
    
    Args:
        df (pd.DataFrame): クリーニング済みの銘柄データ
        index_name (str): 指数名
    """
    print(f"index_history.csv を更新中（指数: {index_name}）...")
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # 現在の銘柄リスト
    current_codes = set(df['code'].tolist())
    
    # 既存のindex_history.csvがあれば読み込み
    if INDEX_HISTORY_FILE.exists():
        history = pd.read_csv(INDEX_HISTORY_FILE)
        
        # 指定された指数の最新状態を取得
        index_history = history[history['index_name'] == index_name].copy()
        
        if len(index_history) > 0:
            # 各銘柄の最新イベントを取得
            latest_events = index_history.sort_values('event_date').groupby('code').tail(1)
            
            # 現在INになっている銘柄
            currently_in = set(latest_events[latest_events['event_type'] == 'IN']['code'].tolist())
            
            # 差分を検出
            newly_added = current_codes - currently_in  # 新規採用
            newly_removed = currently_in - current_codes  # 除外
            
            new_events = []
            
            # 新規採用のイベントを追加
            for code in newly_added:
                new_events.append({
                    'code': code,
                    'index_name': index_name,
                    'event_type': 'IN',
                    'event_date': current_date
                })
            
            # 除外のイベントを追加
            for code in newly_removed:
                new_events.append({
                    'code': code,
                    'index_name': index_name,
                    'event_type': 'OUT',
                    'event_date': current_date
                })
            
            if new_events:
                # 新しいイベントを追記
                new_events_df = pd.DataFrame(new_events)
                history = pd.concat([history, new_events_df], ignore_index=True)
                history.to_csv(INDEX_HISTORY_FILE, index=False, encoding='utf-8-sig')
                
                print(f"[OK] {len(new_events)}件のイベントを追記しました")
                print(f"  - 新規採用: {len(newly_added)}銘柄")
                print(f"  - 除外: {len(newly_removed)}銘柄")
            else:
                print("[OK] 変更なし（差分なし）")
        else:
            # 指定された指数の履歴がない場合は全銘柄をINとして追加
            events = []
            for code in current_codes:
                events.append({
                    'code': code,
                    'index_name': index_name,
                    'event_type': 'IN',
                    'event_date': current_date
                })
            
            events_df = pd.DataFrame(events)
            history = pd.concat([history, events_df], ignore_index=True)
            history.to_csv(INDEX_HISTORY_FILE, index=False, encoding='utf-8-sig')
            
            print(f"[OK] {len(events)}件のイベントを追加しました（初回登録）")
    else:
        # 新規作成: 全銘柄をINとして記録
        events = []
        for code in current_codes:
            events.append({
                'code': code,
                'index_name': index_name,
                'event_type': 'IN',
                'event_date': current_date
            })
        
        history = pd.DataFrame(events)
        history.to_csv(INDEX_HISTORY_FILE, index=False, encoding='utf-8-sig')
        
        print(f"[OK] index_history.csv を作成しました: {INDEX_HISTORY_FILE}")
        print(f"  - イベント数: {len(history)}")


def create_ticker_directories(df):
    """
    各銘柄用のディレクトリを作成
    
    Args:
        df (pd.DataFrame): 銘柄データ
    """
    print("各銘柄のディレクトリを作成中...")
    
    created_count = 0
    for code in df['code']:
        ticker_dir = DATA_DIR / code
        ticker_dir.mkdir(parents=True, exist_ok=True)
        created_count += 1
    
    print(f"[OK] {created_count}個のディレクトリを作成しました")


def main():
    """
    メイン処理
    """
    print("=" * 60)
    print("JP-Stock-Sentinel: 銘柄マスタ初期化（新構造）")
    print("=" * 60)
    print()
    
    try:
        # 1. CSVをダウンロード
        df = download_nikkei_csv()
        print()
        
        # 2. データをクリーニング
        df = clean_and_convert_data(df)
        print()
        
        # 3. stocks_master.csv の更新は update_stocks_from_jpx.py に委譲するためここではスキップ
        # create_or_update_stocks_master(df)
        print("stocks_master.csv の更新は update_stocks_from_jpx.py で行われます。")
        print()
        
        # 4. index_history.csv を作成/更新
        create_or_update_index_history(df, index_name='Nikkei225')
        print()
        
        # 5. 各銘柄のディレクトリを作成
        create_ticker_directories(df)
        print()
        
        print("=" * 60)
        print("[OK] 初期化が完了しました！")
        print("=" * 60)
        print()
        print(f"生成されたファイル:")
        print(f"  - {STOCKS_MASTER_FILE}")
        print(f"  - {INDEX_HISTORY_FILE}")
        print(f"  - {DATA_DIR}/<各銘柄ティッカー>/")
        print()
        
    except Exception as e:
        print(f"[ERROR] エラーが発生しました: {e}")
        raise


if __name__ == "__main__":
    main()
