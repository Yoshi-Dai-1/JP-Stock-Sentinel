import os
import re
import json
from datetime import datetime, timedelta
import requests
import zipfile
from lxml import etree
from bs4 import BeautifulSoup
from pathlib import Path


def should_run_weekly(force=False):
    """
    週次実行チェック。最終実行日から7日以上経過している場合のみTrueを返す。
    
    Args:
        force: Trueの場合、チェックをスキップして常にTrueを返す
        
    Returns:
        bool: 実行すべきかどうか
    """
    if force:
        return True
    
    project_root = Path(__file__).parent.parent
    master_dir = project_root / "data" / "master"
    master_dir.mkdir(parents=True, exist_ok=True)
    
    last_run_file = master_dir / "last_edinet_run.txt"
    
    if not last_run_file.exists():
        return True
    
    try:
        with open(last_run_file, "r") as f:
            last_run_date = datetime.strptime(f.read().strip(), "%Y-%m-%d")
        
        days_since_last_run = (datetime.now() - last_run_date).days
        
        if days_since_last_run >= 7:
            return True
        else:
            print(f"[スキップ] 最終実行日から{days_since_last_run}日しか経過していません（7日以上で実行）")
            return False
    except Exception as e:
        print(f"[警告] 最終実行日の読み込みに失敗: {e}")
        return True


def update_last_run_date():
    """最終実行日を現在日付で更新する"""
    project_root = Path(__file__).parent.parent
    master_dir = project_root / "data" / "master"
    master_dir.mkdir(parents=True, exist_ok=True)
    
    last_run_file = master_dir / "last_edinet_run.txt"
    
    with open(last_run_file, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))
    
    print(f"[更新] 最終実行日を記録しました: {datetime.now().strftime('%Y-%m-%d')}")



class EDINETDownloader:
    """
    EDINET APIを使用してXBRLファイルをダウンロード・解凍するクラス
    """
    
    def __init__(self, api_key=None):
        """
        Args:
            api_key: EDINET APIキー（Noneの場合は環境変数から取得）
        """
        self.api_key = api_key or os.getenv('EDINET_API_KEY')
        self.base_url = "https://disclosure.edinet-fsa.go.jp/api/v2/documents"
        self.save_dir = "downloads"
        self.extract_dir = "extracted_xbrl"
        
        # 必要なフォルダがなければ作成
        os.makedirs(self.save_dir, exist_ok=True)
        os.makedirs(self.extract_dir, exist_ok=True)
    
    def get_doc_id(self, target_date, sec_code):
        """
        日付と証券コードからdocIDを特定する
        
        Args:
            target_date: 対象日付（YYYY-MM-DD形式）
            sec_code: 証券コード（4桁）
            
        Returns:
            docID（文字列）、見つからない場合はNone
        """
        url = f"{self.base_url}.json"
        params = {
            'date': target_date,
            'type': '2',
            'Subscription-Key': self.api_key
        }
        try:
            res = requests.get(url, params=params)
            res.raise_for_status()
            results = res.json().get('results', [])
            
            formatted_code = f"{sec_code}0" if len(str(sec_code)) == 4 else str(sec_code)
            
            # 該当する企業の報告書（120:有報, 130:訂正有報, 180:半期報告書, 190:訂正半期報告書）を抽出
            target_types = ["120", "130", "180", "190"]
            target_docs = [
                doc for doc in results 
                if doc.get('secCode') == formatted_code and doc.get('docTypeCode') in target_types
            ]
            
            if not target_docs:
                return None
            
            # 訂正(130, 190)があればそれを優先し、複数ある場合は提出時間が遅い（提出順序が後）ものを選択
            target_docs.sort(key=lambda x: (x.get('docTypeCode') in ["130", "190"], x.get('submitDateTime')), reverse=True)
            
            selected = target_docs[0]
            if selected.get('docTypeCode') in ["130", "190"]:
                print(f"  [訂正報告書を発見] docID: {selected.get('docID')} (元書類の訂正版)")
            
            return selected.get('docID')
        except Exception as e:
            print(f"docIDの特定中にエラー: {e}")
        return None
    
    def download_xbrl(self, doc_id):
        """
        指定したdocIDのXBRLファイルをダウンロードして展開する
        
        Args:
            doc_id: ドキュメントID
            
        Returns:
            展開先ディレクトリのパス、失敗した場合はNone
        """
        url = f"{self.base_url}/{doc_id}"
        params = {
            'type': '1',
            'Subscription-Key': self.api_key
        }
        
        # docIDごとの個別ディレクトリを作成
        doc_dir = os.path.join(self.extract_dir, doc_id)
        os.makedirs(doc_dir, exist_ok=True)
        zip_path = os.path.join(doc_dir, f"{doc_id}.zip")
        
        try:
            res = requests.get(url, params=params)
            res.raise_for_status()
            with open(zip_path, 'wb') as f:
                f.write(res.content)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(doc_dir)
            
            print(f"  [成功] {doc_id} をダウンロード・展開しました。")
            return doc_dir
        except Exception as e:
            print(f"ダウンロード/展開中にエラー: {e}")
        return None


class QualitativeDataExtractor:
    """
    「事業の内容」、「沿革」、「配当政策」、「株主に対する特典」などの
    定性情報をXBRLおよびHTMLファイルから抽出するクラス
    """
    
    def __init__(self, target_dir="extracted_xbrl"):
        """
        Args:
            target_dir: XBRLファイルが展開されているディレクトリのパス
        """
        self.target_dir = target_dir
        self.files = self._identify_files()
        
        instance_path = self.files["instance"]
        with open(instance_path, 'rb') as f:
            self.tree = etree.parse(f)
    
    def _identify_files(self):
        """XBRLファイルとHTMLファイルを特定する"""
        xbrl_files = []
        html_files = []
        
        for root, dirs, files in os.walk(self.target_dir):
            for file in files:
                full_path = os.path.join(root, file)
                if file.endswith(".xbrl"):
                    xbrl_files.append(full_path)
                elif file.endswith(".htm") and "PublicDoc" in root:
                    html_files.append(full_path)
        
        if not xbrl_files:
            for root, dirs, files in os.walk(self.target_dir):
                for file in files:
                    if file.endswith(".xbrl"):
                        xbrl_files.append(os.path.join(root, file))
        
        if not xbrl_files:
            raise FileNotFoundError(f"ディレクトリ {self.target_dir} 内にXBRLファイルが見つかりません。")
        
        # 本文（jpcrpやjpigp）を含むファイルを優先
        priority_files = [f for f in xbrl_files if "jpcrp" in f or "jpigp" in f]
        instance_path = priority_files[0] if priority_files else xbrl_files[0]
        
        return {
            "instance": instance_path,
            "htmls": html_files
        }
    
    def _clean_html_text(self, html_content):
        """HTMLタグを除去してプレーンテキストを返す"""
        if not html_content:
            return ""
        
        # 実体参照のデコード（簡易版）
        text = html_content.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&quot;', '"').replace('&nbsp;', ' ')
        
        # <br/>などはスペースに
        text = re.sub(r'<br\s*/?>', ' ', text)
        
        # タグの除去
        text = re.sub(r'<[^>]+>', '', text)
        
        # 連続空白と改行の整理
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def get_text_by_name(self, element_names, clean_html=False):
        """
        テキスト情報を取得し、必要に応じてHTMLをクリーンアップする
        
        Args:
            element_names: 検索するXBRL要素名のリスト
            clean_html: HTMLタグをクリーンアップするかどうか
            
        Returns:
            抽出されたテキスト、または None
        """
        text = None
        
        # 1. XBRLツリーからの探索
        for name in element_names:
            elements = self.tree.xpath(f"//*[local-name()='{name}']")
            if elements:
                raw_text = "".join(elements[0].itertext())
                if raw_text:
                    text = raw_text
                    break
        
        # 2. HTMLファイルからの探索 (iXBRL対応)
        if not text and self.files.get('htmls'):
            for html_path in self.files['htmls']:
                if text:
                    break
                try:
                    with open(html_path, "r", encoding="utf-8") as f:
                        soup = BeautifulSoup(f, "lxml")
                        
                        # ix:nonNumeric で name が一致するものを探す
                        def match_tag(attr_name):
                            if not attr_name:
                                return False
                            return any(attr_name.lower().endswith(target.lower()) for target in element_names)
                        
                        found = soup.find("ix:nonnumeric", attrs={"name": match_tag})
                        if found:
                            text = found.get_text()
                            break
                
                except Exception as e:
                    print(f"[Warning] Failed to parse HTML {html_path}: {e}")
        
        if not text:
            return None
        
        if clean_html:
            soup = BeautifulSoup(text, "html.parser")
            for s in soup(["script", "style"]):
                s.decompose()
            text = soup.get_text(separator=" ")
            
            # 「株式事務の概要」から「株主に対する特典」だけを切り出す
            if "OverviewOfOperationalProceduresForSharesTextBlock" in element_names:
                keywords = ["株主に対する特典", "株主優待", "株主特典", "優待制度", "特典の概要"]
                found_idx = -1
                for kw in keywords:
                    idx = text.find(kw)
                    if idx != -1:
                        found_idx = idx
                        break
                
                if found_idx != -1:
                    # キーワード以降を抽出するが、表の終わり（次の項目）までを狙う
                    end_keywords = ["（注）", "単元未満株式", "公告掲載方法", "第７"]
                    end_idx = len(text)
                    for ekw in end_keywords:
                        e_idx = text.find(ekw, found_idx + 10)
                        if e_idx != -1 and e_idx < end_idx:
                            end_idx = e_idx
                    text = text[found_idx:end_idx]
            
            text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_qualitative_data(self):
        """
        定性情報（事業の内容、沿革、配当政策、株主に対する特典）を抽出する
        
        Returns:
            dict: 抽出された定性情報の辞書
        """
        # 抽出対象の定義
        mapping = {
            "事業の内容": (["DescriptionOfBusinessTextBlock"], True),
            "沿革": (["HistoryTextBlock", "CompanyHistoryTextBlock"], True),
            "配当政策": (["DividendPolicyTextBlock"], True),
            "株主に対する特典": (["PrivilegeDetailTextBlock", "OverviewOfOperationalProceduresForSharesTextBlock"], True),
        }
        
        results = {}
        
        for label, (tags, clean_html) in mapping.items():
            val = self.get_text_by_name(tags, clean_html=clean_html)
            if val is not None:
                results[label] = val
        
        return results
    
    def save_qualitative_texts(self, results, sec_code, date, output_dir=None):
        """
        定性情報をJSON形式でdata/{ticker}/info.jsonに保存する。
        履歴データを配列で管理し、最新データを先頭に追加する。
        
        Args:
            results: 抽出結果の辞書
            sec_code: 証券コード（4桁）
            date: 基準日（当事業年度終了日）
            output_dir: 出力ディレクトリ（非推奨、後方互換性のため残す）
        """
        # プロジェクトルートのdataディレクトリを使用
        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data"
        
        # ティッカー形式に変換（例: 7203 → 7203.T）
        ticker = f"{sec_code}.T"
        ticker_dir = data_dir / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        
        info_file = ticker_dir / "info.json"
        
        # 既存のJSONファイルを読み込み、または初期化
        if info_file.exists():
            try:
                with open(info_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, Exception):
                data = {"ticker": ticker}
        else:
            data = {"ticker": ticker}
        
        # 1. 最新上書き項目（常にトップレベルに最新を保持）
        # 事業の内容
        if "事業の内容" in results:
            data["business_description"] = results["事業の内容"]
        # 沿革
        if "沿革" in results:
            data["history_summary"] = results["沿革"]
        
        # 2. 強制履歴追記項目（配列で履歴を保持）
        if "dividend_policy_history" not in data:
            data["dividend_policy_history"] = []
        if "benefits_history" not in data:
            data["benefits_history"] = []
            
        # 配当方針の処理
        if "配当政策" in results:
            new_div = {
                "fiscal_year_end": date,
                "report_date": datetime.now().strftime("%Y-%m-%d"),
                "text": results["配当政策"]
            }
            # 重複チェック（同じ会計年度のデータがあれば追記しない）
            if not any(entry.get("fiscal_year_end") == date for entry in data["dividend_policy_history"]):
                data["dividend_policy_history"].insert(0, new_div) # 最新を先頭に
            else:
                # 既存エントリの更新（必要に応じて最新日付に更新）
                for entry in data["dividend_policy_history"]:
                    if entry.get("fiscal_year_end") == date:
                        entry["text"] = results["配当政策"]
                        entry["report_date"] = datetime.now().strftime("%Y-%m-%d")
                        break

        # 株主に対する特典の処理
        if "株主に対する特典" in results:
            new_ben = {
                "fiscal_year_end": date,
                "report_date": datetime.now().strftime("%Y-%m-%d"),
                "text": results["株主に対する特典"]
            }
            # 重複チェック
            if not any(entry.get("fiscal_year_end") == date for entry in data["benefits_history"]):
                data["benefits_history"].insert(0, new_ben) # 最新を先頭に
            else:
                for entry in data["benefits_history"]:
                    if entry.get("fiscal_year_end") == date:
                        entry["text"] = results["株主に対する特典"]
                        entry["report_date"] = datetime.now().strftime("%Y-%m-%d")
                        break
        
        # 最終更新日の更新
        data["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        
        # 保存
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"  [保存完了] {info_file}")



def extract_from_xbrl(target_dir, sec_code=None, date=None, save_to_file=True):
    """
    XBRLディレクトリから定性情報を抽出する便利関数
    
    Args:
        target_dir: XBRLファイルが展開されているディレクトリのパス
        sec_code: 証券コード（ファイル保存時に使用）
        date: 基準日（ファイル保存時に使用）
        save_to_file: ファイルに保存するかどうか
        
    Returns:
        dict: 抽出された定性情報の辞書
    """
    extractor = QualitativeDataExtractor(target_dir)
    results = extractor.extract_qualitative_data()
    
    if save_to_file and sec_code and date:
        extractor.save_qualitative_texts(results, sec_code, date)
    
    return results


def extract_from_edinet(sec_code, target_date, api_key=None, save_to_file=True):
    """
    EDINETから直接ダウンロードして定性情報を抽出する統合関数
    
    Args:
        sec_code: 証券コード（4桁）
        target_date: 対象日付（YYYY-MM-DD形式）
        api_key: EDINET APIキー（Noneの場合は環境変数から取得）
        save_to_file: ファイルに保存するかどうか
        
    Returns:
        dict: 抽出された定性情報の辞書、失敗した場合はNone
    """
    print(f"\n=== EDINET定性情報抽出開始 ===")
    print(f"証券コード: {sec_code}")
    print(f"対象日付: {target_date}")
    
    # 1. EDINETからダウンロード
    downloader = EDINETDownloader(api_key)
    
    print(f"\n[1/3] docIDを検索中...")
    doc_id = downloader.get_doc_id(target_date, sec_code)
    
    if not doc_id:
        print(f"  [エラー] {target_date} に証券コード {sec_code} の報告書が見つかりませんでした。")
        return None
    
    print(f"  [成功] docID: {doc_id}")
    
    print(f"\n[2/3] XBRLファイルをダウンロード・解凍中...")
    xbrl_dir = downloader.download_xbrl(doc_id)
    
    if not xbrl_dir:
        print(f"  [エラー] ダウンロードに失敗しました。")
        return None
    
    # 2. 定性情報を抽出
    print(f"\n[3/3] 定性情報を抽出中...")
    try:
        extractor = QualitativeDataExtractor(xbrl_dir)
        results = extractor.extract_qualitative_data()
        
        # 基準日を取得（XBRLから）
        date_elem = extractor.tree.xpath("//*[local-name()='CurrentFiscalYearEndDateDEI']")
        fiscal_year_end = date_elem[0].text if date_elem else target_date
        
        if save_to_file:
            extractor.save_qualitative_texts(results, sec_code, fiscal_year_end)
        
        print(f"\n=== 抽出完了 ===")
        print(f"抽出項目数: {len(results)}")
        
        return results
        
    except Exception as e:
        print(f"  [エラー] 抽出中にエラーが発生しました: {e}")
        return None



if __name__ == "__main__":
    # 使用例
    import sys
    
    if len(sys.argv) > 1:
        # コマンドライン引数がある場合
        if sys.argv[1] == "--edinet":
            # EDINETから直接ダウンロード
            if len(sys.argv) < 4:
                print("使用方法: python qualitative_extractor.py --edinet <証券コード> <日付(YYYY-MM-DD)>")
                print("例: python qualitative_extractor.py --edinet 7203 2024-06-25")
                sys.exit(1)
            
            sec_code = sys.argv[2]
            target_date = sys.argv[3]
            
            results = extract_from_edinet(sec_code, target_date)
            
            if results:
                print("\n=== 抽出結果 ===")
                for key, value in results.items():
                    print(f"\n【{key}】")
                    print(value[:200] + "..." if len(value) > 200 else value)
        else:
            # ローカルディレクトリから抽出
            target_dir = sys.argv[1]
            sec_code = sys.argv[2] if len(sys.argv) > 2 else "0000"
            date = sys.argv[3] if len(sys.argv) > 3 else "unknown"
            
            results = extract_from_xbrl(target_dir, sec_code, date)
            
            print("\n=== 抽出結果 ===")
            for key, value in results.items():
                print(f"\n【{key}】")
                print(value[:200] + "..." if len(value) > 200 else value)
    else:
        print("使用方法:")
        print("  1. EDINETから直接ダウンロード:")
        print("     python qualitative_extractor.py --edinet <証券コード> <日付(YYYY-MM-DD)>")
        print("     例: python qualitative_extractor.py --edinet 7203 2024-06-25")
        print("")
        print("  2. ローカルディレクトリから抽出:")
        print("     python qualitative_extractor.py <XBRLディレクトリ> [証券コード] [基準日]")
        print("     例: python qualitative_extractor.py extracted_xbrl/S100XXXX 7203 2024-03-31")

