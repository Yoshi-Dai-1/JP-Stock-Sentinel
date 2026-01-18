---
trigger: always_on
---

# Role
あなたは初心者の私のための、親切で高度な技術を持つPythonエンジニアです。

# Project JP-Stock-Sentinel: Technical Requirements

- 目的: 日経225銘柄の全履歴（株価・決算・予想）を無料で永久保存・可視化する。
- 優先事項: 
  1. インフラコスト0円（GitHub Actions + Streamlit）。
  2. 堅牢性（yfinanceのレート制限を考慮し、1銘柄ずつ丁寧に取得）。
  3. デザイン（Material 3 Expressive準拠）。

# Rule
- 一度に大量のコードを書かず、1機能ずつ実行確認を求めながら進めること。
- 株式分割の調整計算は自作せず、yfinanceの標準機能に頼ること。

## Data Management
- 日経公式CSV (https://indexes.nikkei.co.jp/nkave/archives/file/nikkei_stock_average_weight_jp.csv) をマスターとする。
- CSV内の「日付」列を基準に、銘柄の採用・除外履歴を自動記録する。
- 銘柄ごとに `data/{ticker}/` ディレクトリを作成し、`prices.csv`, `financials.csv`, `estimates.csv` に分割保存する。
- 過去データは最大年数分取得し、以降は日次で「追記・更新」を行う。

## Qualitative Data Integration (From EDINET)
- 既存の `qualitative_extractor.py` のロジックを継承する。
- 抽出したテキストは `data/{ticker}/info.json` に構造化して保存すること。
- 歴史（沿革）や配当政策は、複数年度のデータを配列（Array）として保持し、推移を確認できるようにする。
- GitHub Secrets から 'EDINET_API_KEY' を読み込む設計にする。

## Visualization (Material 3 Expressive)
- Primary: #E8B7BC (Bar), #BD6970 (Line)
- Secondary: #B1C9D6 (Bar), #5689A3 (Line)
- Tertiary: #C0D1B5 (Bar), #74945E (Line)
- Quaternary: #DFCCB9 (Bar), #B18357 (Line)
- Quinary: #D4C7D9 (Bar), #9B7BA6 (Line)
- Rules: Bar opacity 0.7, Line opacity 1.0 (width 2, point radius 3.5).

## Constraints
- 完全無料を維持するため、外部DBは使わずGitHub ActionsとGit LFS/Storageのみで完結させる。