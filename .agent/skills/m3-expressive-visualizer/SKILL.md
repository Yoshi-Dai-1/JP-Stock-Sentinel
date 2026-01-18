---
name: m3-expressive-visualizer
description: Material 3 Expressiveに準拠し、指定された5つのパレット（Primary〜Quinary）を構成要素別に適用して決算グラフを描画します。
---
# Material 3 Financial Visualizer Skill

## 1. 基本配色ルール (General Elements)
グラフの構成要素に応じて、以下の透過度とスタイルを適用する。

- **棒グラフ (Bar)**: 各パレットの『棒用カラー』を使用。 `opacity: 0.7`
- **線グラフ (Line)**: 各パレットの『線用カラー』を使用。 `opacity: 1.0` (ベタ塗り), `borderWidth: 2`
- **点 (Point)**: 線と同じ『線用カラー』を使用。 `opacity: 1.0`, `radius: 3.5`

## 2. カラーパレット定義
可視化する情報の優先順位（例：売上、営業利益、純利益の順など）に応じて、以下のパレットを選択して使用する。

| パレット名 | 棒用カラー (#) | 線用カラー (#) |
| :--- | :--- | :--- |
| **Primary** | #E8B7BC | #BD6970 |
| **Secondary** | #B1C9D6 | #5689A3 |
| **Tertiary** | #C0D1B5 | #74945E |
| **Quaternary** | #DFCCB9 | #B18357 |
| **Quinary** | #D4C7D9 | #9B7BA6 |

## 3. 重要指標の例外ルール (Special: EPS/BPS)
EPS（1株当たり利益）および BPS（1株当たり純資産）のグラフは、棒のみの構成とする。
- **適用カラー**: その指標に割り当てられたパレットの『**棒用カラー**』および『**線用カラー**』のセットを使用。

## 4. デザイン・フィロソフィー
- チャート（価格）ではなく、**決算数値の推移**（四半期・通期）を可視化することに特化する。
- Material 3 Expressive の「ダイナミックな形状」と「一貫性のある色使い」を維持する。