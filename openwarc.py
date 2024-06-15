import gzip
import io
import json
import os
import re
import sys
import tempfile
import traceback
import zlib

import requests
from bs4 import BeautifulSoup
from datasets import Dataset, load_from_disk
from warcio.archiveiterator import ArchiveIterator
from trafilatura import fetch_url, extract, extract_metadata
from tqdm import tqdm

# データセットを格納する場所
output_file = "/mnt/nvme2n1/dataset/commoncrawl"

# 前回実行時、処理が途中で中断された場合にデータセットと進捗を復元する
# 1. データセットのロード
# 2. warc.pathsファイルの読み込み
# 3. 進捗の読み込み

# データセットのロード
try:
    refined_common_crawl = load_from_disk(output_file).to_list()
except Exception:
    refined_common_crawl = []

# 途中から再開する用の位置情報の取得
if len(refined_common_crawl) > 0:
    final_id = refined_common_crawl[-1]["commoncrawl_id"]
else:
    final_id = 0

# warc.pathsファイルの読み込み
# これによって全てのwarcファイルの名前が分かる
warc_path_file_location = "data/202404/warc.paths"
with open(warc_path_file_location, "r", encoding="utf-8") as f:
    warc_paths = f.read().splitlines()

# 進捗の読み込み
# 進捗データは処理済みセグメントファイル名と最後に読み込んでいたwarcファイルの処理済みイテレーション回数の２つ
# もし進捗ファイルが読み込めない場合は新しく作成する
processed_file_names = set()

try:
    with open("progress.txt", "r", encoding="utf-8") as f:
        obj = json.loads(f.read())
        processed_file_names = set(obj["processed_file_names"])
        last_itr_count = obj["last_itr_count"]
except Exception as e:
    print(e)
    print("Create New.")
    processed_file_names = set()
    last_itr_count = 0

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


# メインの処理開始
# warcファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
#     処理手順:
#     1. warcファイルをダウンロード。（メモリ上に）
#     2. ダウンロードしたファイルをメモリ上に解凍
#     3. 解凍したデータをイテレートする
#     4. 日本語を対象として配列に追加
try:
    iteration = 0
    lang_pattern = re.compile(r'<html.*lang="(.*?)"')
    with tqdm(total=len(cleaned_warcs), unit='file', unit_scale=True, position=0) as pbar:
        for warc_path in cleaned_warcs:
            if warc_path in processed_file_names:
                pbar.update()
                continue

            # WARCファイルのURLを構築
            warc_url = f"https://data.commoncrawl.org/{warc_path}"

            # WARCファイルをダウンロード
            response = requests.get(warc_url, stream=True)
            total_size = int(response.headers.get("Content-Length", 0))
            block_size = 1024 * 1024  # 1 KB
            decompressobj = zlib.decompressobj(zlib.MAX_WBITS|32)

            content = bytearray()
            is_response_accepted = False
            tmp_result = None
            for record in tqdm(ArchiveIterator(response.raw), position=1):
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
                        # パースに失敗することがある
                        try:
                            result = json.loads(json_data)
                        except:
                            continue

                        # （Swallowより）本文の文字数が400以下の場合は低品質とみなしてスキップ
                        if len(result["text"]) < 400:
                            result["rejected"] = True
                            result["rejected_reason"] = "Too_Short"
                        else:
                            result["rejected"] = False
                            result["rejected_reason"] = ""

                        # 辞書に情報を追加
                        result["commoncrawl_id"] = final_id
                        tmp_result = result
                        is_response_accepted = True

                elif is_response_accepted and record.rec_type == 'metadata':
                    metadata = parse_metadata(record.content_stream().read())
                    if any([item["code"] == "ja" for item in metadata["languages-cld2"]["languages"]]):
                        tmp_result["metadata"] = metadata
                        refined_common_crawl.append(tmp_result)
                    is_response_accepted = False

            # 処理したデータを格納する配列に追加
            processed_file_names.add(warc_path)
            # リソースの解放（念のため）
            del response
            pbar.update()

except Exception as e:
    traceback.print_exc()
finally:
    # 処理結果を保存
    dataset = Dataset.from_list(refined_common_crawl)
    dataset.save_to_disk(output_file)

    # 進捗データの保存
    progression = {"last_itr_count": max(iteration, last_itr_count), "processed_file_names": list(processed_file_names)}
    with open("progress.txt", "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
