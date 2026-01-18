"""
東証公式サイトから上場銘柄一覧（Excel）を取得し、stocks_master.csvを更新するスクリプト

このスクリプトは以下の処理を行います:
1. 東証公式サイトからExcelファイルをダウンロード
2. データを解析して必要な情報を抽出
   - 銘柄名
   - 33業種区分
   - 17業種区分
   - 市場・商品区分
3. stocks_master.csv を更新（差分更新）
"""

import os
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import io

# 定数定義
# 直近月末の東証上場銘柄一覧
JPX_EXCEL_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MASTER_DIR = DATA_DIR / "master"
STOCKS_MASTER_FILE = MASTER_DIR / "stocks_master.csv"

def download_jpx_excel():
    """
    東証公式サイトからExcelをダウンロードし、DataFrameとして返す
    """
    print("東証上場銘柄一覧(Excel)をダウンロード中...")
    response = requests.get(JPX_EXCEL_URL)
    response.raise_for_status()
    
    # Excelファイルを読み込む
    try:
        # .xlsファイルはデフォルトエンジンやxlrdが必要
        # xlrdはバージョンによって.xlsx非対応だったり、古い.xlsのみだったりする
        df = pd.read_excel(io.BytesIO(response.content))
    except Exception as e:
        print(f"Excel読み込みでエラー発生: {e}")
        # エンジンを指定して再試行してみる
        try:
             df = pd.read_excel(io.BytesIO(response.content), engine='xlrd')
        except Exception as e2:
             print(f"xlrdエンジンでもエラー: {e2}")
             raise e

    print(f"[OK] {len(df)}行のデータを取得しました")
    return df

def clean_jpx_data(df):
    """
    JPXデータをクリーニングして必要なカラムを抽出
    """
    print("JPXデータをクリーニング中...")
    
    # カラム名の特定
    rename_map = {
        'コード': 'code',
        '銘柄名': 'name',
        '市場・商品区分': 'market_segment',
        '33業種区分': 'sector_33',
        '17業種区分': 'sector_17'
    }
    
    # 公式ファイルの列名が変わる可能性も考慮してチェック
    # 2024年時点では 'コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分' 等が含まれる
    
    missing_cols = [col for col in rename_map.keys() if col not in df.columns]
    if missing_cols:
        print(f"[警告] 一部のカラムが見つかりません: {missing_cols}")
        print(f"実際のカラム: {df.columns.tolist()}")
        # 続行不可
        raise ValueError(f"必須カラムが見つかりません: {missing_cols}")

    # リネーム
    df = df.rename(columns=rename_map)
    
    # 必要なカラムのみ抽出
    target_cols = ['code', 'name', 'market_segment', 'sector_33', 'sector_17']
    df = df[target_cols].copy()
    
    # コードの整形: 4桁の数字であることを確認し、.Tをつける
    def format_code(code):
        try:
            # コードが数値で来る場合と文字列で来る場合に対処
            code_str = str(int(code))
            return f"{code_str}.T"
        except (ValueError, TypeError):
            return None

    df['code'] = df['code'].apply(format_code)
    
    # コードが無効な行（None）を削除
    df = df.dropna(subset=['code'])
    
    # sectorの欠損値を '-' で埋める
    df['sector_33'] = df['sector_33'].fillna('-')
    df['sector_17'] = df['sector_17'].fillna('-')
    df['market_segment'] = df['market_segment'].fillna('-')
    df['name'] = df['name'].fillna('')

    # 文字列のクリーニング関数
    import unicodedata
    def clean_text(text):
        if not isinstance(text, str):
            return text
        # NFKC正規化（全角英数を半角になど。例: １ -> 1, Ａ -> A）
        text = unicodedata.normalize('NFKC', text)
        
        # 制御文字や特殊な空白を除去・置換
        # \xa0 (NBSP) -> space
        text = text.replace('\xa0', ' ')
        # \u3000 (全角スペース) -> space
        text = text.replace('\u3000', ' ')
        
        # 不可視文字などを除去 (C系統の制御文字)
        # category 'Cf' (Format), 'Cc' (Control) などを削除
        # ただし、Excel由来だとゼロ幅スペース(\u200b)などが混ざることがある
        text = text.replace('\u200b', '')
        
        return text.strip()

    # 文字列カラムに適用
    str_cols = ['name', 'market_segment', 'sector_33', 'sector_17']
    for col in str_cols:
        df[col] = df[col].apply(clean_text)
    
    print(f"[OK] {len(df)}銘柄のデータを整形・クリーニングしました")
    return df

def update_stocks_master(jpx_df):
    """
    stocks_master.csv を更新
    """
    print("stocks_master.csv を更新中...")
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    jpx_df['is_active'] = True
    jpx_df['last_updated'] = today_str
    
    # 既存のマスタがある場合
    if STOCKS_MASTER_FILE.exists():
        old_master = pd.read_csv(STOCKS_MASTER_FILE)
        
        # 既存マスタに新カラムがない場合に追加（スキーマ変更対応）
        new_cols = ['sector_33', 'sector_17', 'market_segment', 'last_updated']
        for col in new_cols:
            if col not in old_master.columns:
                old_master[col] = ''
        
        # 今回の要件: "dbのセクターを廃止" -> 'sector' 列があるなら削除する方向か無視するか
        # ここでは最終的な出力から除外することで廃止とする
        
        # コードをインデックスに設定してマージ準備
        jpx_df_indexed = jpx_df.set_index('code')
        old_master_indexed = old_master.set_index('code')
        
        # outer join
        merged = old_master_indexed.join(jpx_df_indexed, how='outer', lsuffix='_old', rsuffix='_new')
        
        final_df = pd.DataFrame(index=merged.index)
        
        # 更新ロジック: JPXデータ(_new)があればそれを採用、なければ古いデータ(_old)を維持
        # Name
        final_df['name'] = merged['name_new'].fillna(merged['name_old'])
        
        # 新規カラム (sector_33, sector_17, market_segment)
        # 既存データには入っていない可能性が高いのでJPXデータが頼りだが、
        # 過去に一度JPXから取得していれば _old に入っているはず
        final_df['sector_33'] = merged['sector_33_new'].fillna(merged['sector_33_old']).fillna('-')
        final_df['sector_17'] = merged['sector_17_new'].fillna(merged['sector_17_old']).fillna('-')
        final_df['market_segment'] = merged['market_segment_new'].fillna(merged['market_segment_old']).fillna('-')
        
        # last_updated
        final_df['last_updated'] = merged['last_updated_new'].fillna(merged['last_updated_old']).fillna(today_str)
        
        # is_active
        # JPXのリストに含まれているならTrue、含まれていないならFalse（上場廃止 or データ不備）
        # ただし、JPXのリストに含まれない = 上場廃止 とみなしてよいか？
        # 要件「差分更新」: 既存にあってJPXにないものは消さずに is_active=False にするのが安全
        final_df['is_active'] = merged.index.isin(jpx_df_indexed.index)
        
        # インデックスをカラムに戻す
        final_df = final_df.reset_index()
        
    else:
        # 新規作成
        final_df = jpx_df.copy()
        final_df = final_df.reset_index(drop=True)
    
    # カラム順序を整理 (sectorは含めない)
    output_cols = ['code', 'name', 'market_segment', 'sector_33', 'sector_17', 'is_active', 'last_updated']
    final_df = final_df[output_cols]
    
    # 保存
    final_df.to_csv(STOCKS_MASTER_FILE, index=False, encoding='utf-8-sig')
    
    print(f"[OK] stocks_master.csv を更新しました: {STOCKS_MASTER_FILE}")
    print(f"  - 総銘柄数: {len(final_df)}")
    print(f"  - 上場中: {len(final_df[final_df['is_active'] == True])}")


if __name__ == "__main__":
    try:
        df = download_jpx_excel()
        df = clean_jpx_data(df)
        update_stocks_master(df)
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
