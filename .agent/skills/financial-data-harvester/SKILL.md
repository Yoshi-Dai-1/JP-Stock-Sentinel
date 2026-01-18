---
name: financial-data-harvester
description: yfinanceとEDINET APIを使用して、市場データと定性情報を収集・保存します。
---
# Financial Data Harvester Skill
市場データと有価証券報告書からのテキストデータをハイブリッドで取得します。

## データ取得ルール
- **市場データ**: yfinanceを使用。`auto_adjust=True` を必須とし、分割調整後の価格を取得する。
- **定性情報**: 以前の `qualitative_extractor.py` のロジックを使用し、EDINETから取得する。
- **保存形式**:
    - 株価/財務数値: `prices.csv`
    - テキスト（事業内容等）: `info.json`
- **レート制限**: 大量取得時は銘柄間に適度なスリープを挟み、Yahoo Financeからのブロックを回避する。

## データ品質管理 (Data Reliability)
全ての取得データ（銘柄詳細、財務テキストなど）に対して、以下のクリーニング処理を必ず適用します。

1. **NFKC正規化 (Normalization)**
   - 全角英数字を半角に統一（例: `Ａ` → `A`, `１` → `1`）
   - 特殊文字の展開（例: `㈱` → `(株)`）
   - 検索性の向上と表記ゆれの防止のため

2. **空白文字の正規化**
   - 全角スペース(`\u3000`)やNBSP(`\xa0`)を半角スペースに置換
   - 不可視文字（ゼロ幅スペース、制御文字等）の削除

## 日次株価データ取得

### 実行スクリプト
`src/daily_harvester.py` を使用して、全銘柄の株価データを段階的に取得します。

### 進捗管理
`stocks_master.csv` の `last_price_update` カラムで各銘柄の最終更新日時を管理します。
- 未取得または古い銘柄から優先的に処理
- 1回の実行で最大100銘柄を処理
- 取得成功時に `last_price_update` を現在日時に更新

### レート制限対策
- 各銘柄取得後にランダムスリープ（1〜3秒）
- Yahoo Financeのブロックを回避
- 中断・再開が可能な設計

### データ保存
- 保存先: `data/{ticker}/prices.csv`
- yfinanceの `auto_adjust=True` で分割調整済み価格を取得
- 既存データとマージして重複を排除

