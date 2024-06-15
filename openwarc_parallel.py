import argparse
import concurrent
import gzip
import io
import json
import logging
import math
import os
import re
import signal
import tempfile
import time
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
    refined_common_crawl = load_from_disk(dataset_dir).to_list()
except Exception:
    refined_common_crawl = []

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
except Exception as e:
    print(e)
    print("Create New.")
    processed_file_names = []

# 処理していないセグメントファイル名の一覧を取得
cleaned_warcs = []
for warc_path in warc_paths:
    if warc_path not in processed_file_names:
        cleaned_warcs.append(warc_path)

def parse_metadata(byte_array):
    """
    warcのmetadataをdictにパースする
    :param byte_array: record.content_stream().read()
    :return:
    """
    # バイト列を文字列にデコード
    data_str = byte_array.decode('utf-8')

    # 文字列を行ごとに分割
    lines = data_str.split('\r\n')

    # メタデータを格納する辞書
    metadata = {}

    for line in lines:
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # valueがJSON形式のテキストならdictionaryに変換
            try:
                json_data = json.loads(value)
                metadata[key] = json_data
            except json.JSONDecodeError:
                metadata[key] = value

    return metadata


def process_warc(warc_path):
    """
    warcファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
    処理手順:
    1. warcファイルをダウンロード。（メモリ上に）
    2. ダウンロードしたファイルをメモリ上に解凍
    3. 解凍したデータをイテレートする
    4. 日本語を対象として配列に追加
    :param warc_path: warcファイルの場所
    :return: (is_succeed, warc_path, ja_soup_list)
    is_succeed: bool - 処理が成功したかどうか。なんらかの例外が発生するとFalseになる
    warc_path: str - 処理対象のwarcファイル名。入力のwarc_pathと同じ
    ja_soup_list: list[dict] - 処理済みのデータ
    """
    print(f"Start: {warc_path}")
    result_list = []

    try:
        # WARCファイルのURLを構築
        warc_url = f"https://data.commoncrawl.org/{warc_path}"

        # WARCファイルをダウンロード
        response = requests.get(warc_url, stream=True)
        # 403 (Rate limit)と404 (not found)を想定
        # 404の場合は例外を出す
        while response.status_code != 200:
            if response.status_code == 404:
                raise Exception(f"invalid warc url: {warc_url}")
            print("retrying...")
            time.sleep(5)
            response = requests.get(warc_url, stream=True)

        is_response_accepted = False
        tmp_result = None
        lang_pattern = re.compile(r'<html.*lang="(.*?)"')
        for record in ArchiveIterator(response.raw):
            if record.rec_type == 'response' and record.http_headers.get_header('Content-Type') == 'text/html':
                content = record.content_stream().read()
                lang = lang_pattern.search(str(content))

                if lang and lang.group(1) == "ja":
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

                    # （Swallowより）本文の文字数が400以下の場合は低品質とみなしてスキップ
                    if len(result["text"]) < 400:
                        result["rejected"] = True
                        result["rejected_reason"] = "Too_Short"
                    else:
                        result["rejected"] = False
                        result["rejected_reason"] = ""

                    tmp_result = result
                    is_response_accepted = True

            elif is_response_accepted and record.rec_type == 'metadata':
                metadata = parse_metadata(record.content_stream().read())
                if "languages-cld2" not in metadata or "languages" not in metadata["languages-cld2"]:
                    is_response_accepted = False
                    continue
                if any([item["code"] == "ja" for item in metadata["languages-cld2"]["languages"]]):
                    tmp_result["metadata"] = metadata
                    result_list.append(tmp_result)
                    is_response_accepted = False

        return True, warc_path, result_list
    except ParserRejectedMarkup as e:
        return False, warc_path, result_list
    except Exception as e:
        traceback.print_exc()
        return False, warc_path, result_list


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
            if result[0]:
                # もし処理が成功していたら処理済みファイル名に追加し、結果を格納する配列に保存
                refined_common_crawl.extend(result[2])
                processed_file_names.append(result[1])

        # データセットに保存
        if len(refined_common_crawl) > 0:
            dataset = Dataset.from_list(refined_common_crawl)
            dataset.save_to_disk(dataset_dir)

        # 進捗データの保存
        progression = {"processed_file_names": processed_file_names}
        with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
            json.dump(progression, f, ensure_ascii=False)

    # ProcessPoolExecutorによる処理を中断
    executor.shutdown(wait=True)


try:
    # 並列処理の実行
    with tqdm(total=total_iterations, unit='file', unit_scale=True) as pbar:
        with ProcessPoolExecutor(max_workers=args.num_proc) as executor:
            # InterruptとTerminateのハンドラを設定
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            try:
                futures = {executor.submit(process_warc, warc_path): warc_path for warc_path in cleaned_warcs}
                results = []
                # tqdmで進捗を表示したかったので処理が終わり次第実行されるやつを使う
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()  # ここでのresultは(bool, str, list[dict])
                    results.append(result)
                    pbar.update(1)
            except:
                traceback.print_exc()

except Exception as e:
    traceback.print_exc()
finally:
    # 結果を結合
    for result in results:
        if result[0]:
            # もし処理が成功していたら処理済みファイル名に追加し、結果を格納する配列に保存
            refined_common_crawl.extend(result[2])
            processed_file_names.append(result[1])

    # データセットに保存
    dataset = Dataset.from_list(refined_common_crawl)
    print("Saving...")
    dataset.save_to_disk(dataset_dir)

    # 進捗データの保存
    progression = {"processed_file_names": processed_file_names}
    with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
