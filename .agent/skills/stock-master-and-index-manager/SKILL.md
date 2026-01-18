---
name: stock-master-and-index-manager
description: 東証および日経のデータを管理し、銘柄マスタと指数採用履歴を更新します。
---
# Stock Master & Index Manager Skill

東証公式サイトおよび日経公式サイトからデータを取得し、プロジェクト全体のマスタデータと指数履歴を管理します。

## マスタデータの管理

### データソース
**東証上場銘柄一覧 (Excel)** を最優先の情報源（Primary Source）とします。
- 取得頻度: 毎月（第3営業日午前9時以降に更新されるデータを取得）
- 処理スクリプト: `src/update_stocks_from_jpx.py`

### データ品質管理 (Data Reliability)
全ての取得・管理するデータに対して、以下のクリーニング処理を必ず適用します。

1. **NFKC正規化 (Normalization)**
   - 全角英数字を半角に統一（例: `Ａ` → `A`, `１` → `1`）
   - 特殊文字の展開（例: `㈱` → `(株)`）
   - VS Code等での「まぎらわしい文字」警告を回避し、検索性を向上させるため

2. **空白文字の正規化**
   - 全角スペース(`\u3000`)やNBSP(`\xa0`)を半角スペースに置換
   - 不可視文字（ゼロ幅スペース、制御文字等）の削除

### stocks_master.csv (銘柄マスタ)
カラム構成:
- `code`: ティッカーコード（例: 7203.T）
- `name`: 銘柄名
- `market_segment`: 市場・商品区分（例: プライム、グロース）
- `sector_33`: 33業種区分（例: 輸送用機器）
- `sector_17`: 17業種区分（例: 自動車・輸送機）
- `is_active`: 上場中かどうか (True/False)
- `last_updated`: 最終更新日

> **注意**: 旧来の `sector` カラムは廃止され、より詳細な `sector_33`, `sector_17` に移行しました。

---

## 指数履歴の管理

### データソース
**日経平均構成銘柄一覧 (CSV)** を使用します。
- 処理スクリプト: `src/init_nikkei_list.py`

### index_history.csv (指数採用履歴)
「どの銘柄が・いつ・どの指数に・採用/除外されたか」をイベント形式で記録します。

カラム構成:
- `code`: ティッカーコード
- `index_name`: 指数名 (Nikkei225, TOPIX等)
- `event_type`: イベントタイプ (IN / OUT)
- `event_date`: イベント発生日

**記録ルール**:
- 1アクション = 1行
- 差分のみを追記
- マスタデータ(`stocks_master.csv`)はここでは更新せず、参照のみを行う（整合性チェックなど）

---

## 運用ワークフロー

1. **マスタ更新 (月次/随時)**
   `python src/update_stocks_from_jpx.py`
   - 全上場銘柄のリストを最新化
   - 新規上場、上場廃止、市場区分変更を反映

2. **指数構成更新 (日次/随時)**
   `python src/init_nikkei_list.py`
   - 日経225の構成変更を検知し、履歴(`index_history.csv`)に追記
   - ※将来的にはTOPIX等の更新処理もここに含まれるか、別スクリプトで `index_history.csv` に書き込む

## ディレクトリ構成
- `data/master/stocks_master.csv`: 銘柄マスタ
- `data/master/index_history.csv`: 指数履歴
- `data/{ticker}/`: 各銘柄ごとのデータ格納用ディレクトリ