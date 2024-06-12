import argparse
import concurrent
import gzip
import io
import json
import logging
import math
import os
import signal
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import requests
from bs4 import BeautifulSoup, ParserRejectedMarkup, MarkupResemblesLocatorWarning, XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning
from datasets import Dataset, load_from_disk
from warcio.archiveiterator import ArchiveIterator
from trafilatura import fetch_url, extract, extract_metadata
from tqdm import tqdm

# BeautifulSoupのWaningが非常にうるさいので抑制
# ただし"Some characters could not be decoded, and were replaced with REPLACEMENT CHARACTER."の1文はハードコードされており消すことができない
import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module='bs4')
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning, module='bs4')
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module='bs4')
warnings.filterwarnings("ignore", module='bs4')

# TBD
hf_token = os.environ.get("HF_TOKEN", "")

# 実行時引数の設定
parser = argparse.ArgumentParser(description='Process WARC files.')
parser.add_argument('--working_dir', type=str, help='Path to the working_dir.')
parser.add_argument('--dataset_dir', type=str, help='Path to the load and save dataset (not common crawl warc.')
parser.add_argument('--num_proc', type=int, default=8, help='Path to the load and save dataset (not common crawl warc.')
args = parser.parse_args()

working_dir = args.working_dir
dataset_dir = args.dataset_dir

# 前回実行時、処理が途中で中断された場合にデータセットと進捗を復元する
# 1. データセットのロード
# 2. warc.pathsファイルの読み込み
# 3. 進捗の読み込み

# データセットのロード
try:
    ja_soup_list = load_from_disk(dataset_dir).to_list()
except Exception:
    ja_soup_list = []

# warc.pathsファイルの読み込み
# これによって全てのwarcファイルの名前が分かる
with open(os.path.join(working_dir, "data/202404/warc.paths"), "r", encoding="utf-8") as f:
    warc_paths = f.read().splitlines()

# 進捗の読み込み
# 進捗データは処理済みセグメントファイル名と、`dict[セグメントファイル名]=イテレーション回数`の辞書の２つ
# もし進捗ファイルが読み込めない場合は新しく作成する
try:
    with open(os.path.join(working_dir, "progress_parallel.txt"), "r", encoding="utf-8") as f:
        obj = json.loads(f.read())
        processed_file_names = obj["processed_file_names"]
        last_itr_counts = obj["last_itr_counts"]
except Exception as e:
    print(e)
    print("Create New.")
    processed_file_names = []
    last_itr_counts = {}

# 処理していないセグメントファイル名の一覧を取得
cleaned_warcs = []
for warc_path in warc_paths:
    if warc_path not in processed_file_names:
        cleaned_warcs.append(warc_path)


def process_warc(warc_path, last_itr_count):
    """
    warcファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
    処理手順:
    1. warcファイルをダウンロード。（メモリ上に）
    2. ダウンロードしたファイルをメモリ上に解凍
    3. 解凍したデータをイテレートする
    4. 日本語を対象として配列に追加
    :param warc_path: warcファイルの場所
    :param last_itr_count: 前回実行時に、そのwarcファイルがどこまでイテレートされたか。
    :return: (is_succeed, warc_path, last_itr_count, ja_soup_list)
    is_succeed: bool - 処理が成功したかどうか。なんらかの例外が発生するとFalseになる
    warc_path: str - 処理対象のwarcファイル名。入力のwarc_pathと同じ
    last_itr_count: int - 前回実行時に、そのwarcファイルがどこまでイテレートされたか。入力のlast_itr_countと同じ
    ja_soup_list: list[dict] - 処理済みのデータ
    """
    print(f"Start: {warc_path}")
    ja_soup_list = []

    try:
        # WARCファイルのURLを構築
        warc_url = f"https://data.commoncrawl.org/{warc_path}"

        # WARCファイルをダウンロード
        response = requests.get(warc_url, stream=True)
        total_size = int(response.headers.get("Content-Length", 0))
        block_size = 1024  # 1 KB

        content = bytearray()
        for data in response.iter_content(block_size):
            content.extend(data)

        # ダウンロードしたWARCファイルを解凍
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz_file:
            warc_data = gz_file.read()

        # ArchiveIteratorでイテレートする
        iteration = 0
        for record in ArchiveIterator(io.BytesIO(warc_data)):
            iteration += 1
            # 前回処理が終了した時点までイテレート
            if iteration <= last_itr_count:
                last_itr_count = 0
                continue

            # 正常に取得できていて、Content-Typeが"text/html"のものを対象とする
            if record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    content = record.content_stream().read()

                    # HTMLタグのlang属性の値を取得するためにBeautifulSoupを使用
                    # lang="ja"なら日本語ページとみなす (Swallowにならって）
                    soup = BeautifulSoup(content, 'html.parser')

                    html_tag = soup.find('html')
                    if html_tag and html_tag.has_attr('lang'):
                        lang = html_tag['lang']
                        if lang == "ja":
                            # 本文の抽出にはtrafilaturaを用いる。（抽出精度が高いため）
                            # include_formatting=Trueにすることで、抽出したテキストがMarkdown形式になる（h2タグが見出しになったり、テーブルがパースされたり）
                            # deduplicateの効果は不明
                            json_data = extract(content, output_format='json', target_language="ja",
                                                deduplicate=True,
                                                include_formatting=True, include_tables=True)
                            # パースに失敗するとNoneが返ってくるので除外
                            if json_data == None:
                                continue
                            result = json.loads(json_data)

                            # （Swallowより）本文の文字数が400以下の場合は低品質とみなす
                            # その場合、rejectedのフラグを立て、reasonには"Too_Short"を記録する。
                            rejected = False
                            rejected_reason = ""
                            if len(result["text"]) < 400:
                                rejected = True
                                rejected_reason = "Too_Short"

                            # 辞書に情報を追加
                            result["rejected"] = rejected
                            result["rejected_reason"] = rejected_reason
                            result["language"] = "ja"
                            result["commoncrawl_id"] = 0
                            result["url"] = record.rec_headers.get_header('WARC-Target-URI')
                            ja_soup_list.append(result)
                            # print(f"Found Japanese: \n\tURL: {result['url']}\n\tTitle: {result['title']}")

        # リソースの解放（念のため）
        del response
        del warc_data

        return True, warc_path, None, ja_soup_list
    except ParserRejectedMarkup as e:
        return False, warc_path, last_itr_count, ja_soup_list
    except Exception as e:
        traceback.print_exc()
        return False, warc_path, last_itr_count, ja_soup_list


# 非同期処理によって今回新しく得たデータはresultsに格納される
# resultsはList[Dict]
# dictのカラム、データ形式はREADMEを参照
results = None

# 進捗バー表示のための全体のデータ数
total_iterations = len(cleaned_warcs)

def signal_handler(sig, frame):
    """
    SIGINTやSIGTERMが実行されたときに安全にデータを保存して複数プロセスで行っている処理をシャットダウンする。
    これが無いとPCごと逝く

    :param sig:
    :param frame:
    :return:
    """
    print('Ctrl+C pressed. Shutting down gracefully...')

    if results != None and len(results) > 0:
        print("Saving...")

        # 結果を結合
        for result in results:
            ja_soup_list.extend(result[3])
            if result[0]:
                # もし処理が成功していたら処理済みファイル名に追加し、処理途中の進捗データから削除
                processed_file_names.append(result[1])
                if result[1] in last_itr_counts:
                    del last_itr_counts[result[1]]
            else:
                # 処理失敗の場合は処理途中の進捗データに追加
                last_itr_counts[result[1]] = result[2]

        # データセットに保存
        dataset = Dataset.from_list(ja_soup_list)
        dataset.save_to_disk(dataset_dir)

        # 進捗データの保存
        progression = {"last_itr_counts": last_itr_counts, "processed_file_names": processed_file_names}
        with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
            json.dump(progression, f, ensure_ascii=False)

    # ProcessPoolExecutorによる処理を中断
    executor.shutdown(wait=True)

try:
    # TBD
    iteration = 0
    warc_with_info = []

    # 各warcファイルに対して、どのイテレーション回数まで処理が進んでいるかの情報を付与
    # 各Warcファイルに対して実行する関数に「warcファイル名」と「どこから処理をはじめるか」の２つを渡すため
    for warc_path in cleaned_warcs:
        last_itr_count = last_itr_counts[warc_path] if warc_path in last_itr_counts else 0
        warc_with_info.append((warc_path, last_itr_count))

    # 並列処理の実行
    with tqdm(total=total_iterations, unit='file', unit_scale=True) as pbar:
        with ProcessPoolExecutor(max_workers=args.num_proc) as executor:
            # InterruptとTerminateのハンドラを設定
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            try:
                futures = {executor.submit(process_warc, warc_path, last_itr_count): (warc_path, last_itr_count)
                           for warc_path, last_itr_count in warc_with_info}
                results = []
                # tqdmで進捗を表示したかったので処理が終わり次第実行されるやつを使う
                for future in concurrent.futures.as_completed(futures):
                    warc_path, last_itr_count = futures[future]
                    result = future.result() # ここでのresultは(bool, str, int, list[dict])
                    results.append(result)
                    pbar.update(1)
            except:
                traceback.print_exc()

except Exception as e:
    traceback.print_exc()
finally:
    # 結果を結合
    for result in results:
        ja_soup_list.extend(result[3])
        if result[0]:
            # もし処理が成功していたら処理済みファイル名に追加し、処理途中の進捗データから削除
            processed_file_names.append(result[1])
            if result[1] in last_itr_counts:
                del last_itr_counts[result[1]]
        else:
            # 処理失敗の場合は処理途中の進捗データに追加
            last_itr_counts[result[1]] = result[2]

    # データセットに保存
    dataset = Dataset.from_list(ja_soup_list)
    print("Saving...")
    dataset.save_to_disk(dataset_dir)

    # 進捗データの保存
    progression = {"last_itr_counts": last_itr_counts, "processed_file_names": processed_file_names}
    with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
