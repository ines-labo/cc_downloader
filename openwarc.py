import json
import json
import os
import re
import time
import traceback
import zlib
from typing import Optional
import logging

import requests
import zstandard
from datasets import Dataset
from fasttext import load_model
from tqdm import tqdm
from trafilatura import extract
from ulid import ULID
from warcio.archiveiterator import ArchiveIterator

from lang_predictor import FastTextLangPredictor
from xml_parser import XMLMetadataParser


# trafilaturaによるwarningを抑制
logging.getLogger("trafilatura.utils").setLevel(logging.ERROR)
logging.getLogger("trafilatura.core").setLevel(logging.ERROR)

# データセットを格納する場所
output_folder_name = "/mnt/nvme2n1/dataset/commoncrawl/shards"

# 前回実行時、処理が途中で中断された場合にデータセットと進捗を復元する
# 1. warc.pathsファイルの読み込み
# 2. 進捗の読み込み

# warc.pathsファイルの読み込み
# これによって全てのwarcファイルの名前が分かる
warc_path_file_location = "data/202404/warc.paths"
with open(warc_path_file_location, "r", encoding="utf-8") as f:
    warc_paths = f.read().splitlines()

# 進捗の読み込み
# 進捗データは処理済みセグメントファイル名と最後に読み込んでいたwarcファイルの処理済みイテレーション回数の２つ
# もし進捗ファイルが読み込めない場合は新しく作成する
processed_file_names = set()

# 一時ファイルのパス
temp_file_path = "./temp_refined_warc_samples.jsonl"

# 1つのzstdに含めたい最大のwarcファイル件数
zstd_chunk_size = 1000

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


def get_file_size(path):
    return os.path.getsize(path)


def save_refined(refined_data, path):
    mode = "a"
    if not os.path.exists(path):
        mode = "w"
    # 一時ファイルにJSONLとして書き込む
    with open(path, mode, encoding="utf-8") as f:
        for item in refined_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def clear_tmp_file(path, create_empty=True):
    try:
        if os.path.exists(path):
            os.remove(path)
        if create_empty:
            open(path, "w", encoding="utf-8").close()
    except Exception:
        traceback.print_exc()


def compress(src_path, output_folder_path):
    os.makedirs(output_folder_path, exist_ok=True)
    output_file_name = os.path.join(output_folder_path, str(ULID()) + ".zst")
    with open(src_path, "r", encoding="utf-8") as src_f, open(output_file_name, "wb") as out_f:
        cctx = zstandard.ZstdCompressor()
        with cctx.stream_writer(out_f) as compressor:
            for line in src_f:
                compressor.write(line.encode("utf-8"))
            compressor.flush()


def lang_detect(xml_data, metadata_parser: XMLMetadataParser, lang_detector: FastTextLangPredictor):
    meta_description = metadata_parser.parse_description(xml_data)
    if meta_description and len(meta_description) > 10:
        return lang_detector.predict(meta_description.replace("\n", " "))
    meta_title = metadata_parser.parse_title(xml_data)
    if meta_title and len(meta_title) > 5:
        return lang_detector.predict(meta_title.replace("\n", " "))
    meta_heading = metadata_parser.parse_heading(xml_data)
    if meta_heading and len(meta_heading) > 10:
        return lang_detector.predict(meta_heading.replace("\n", " "))
    return None


def download_warc_file(warc_url, max_retries=5, retry_delay=5):
    """
    WARCファイルをダウンロードする関数

    :param warc_url: ダウンロードするWARCファイルのURL
    :param max_retries: 最大リトライ回数（デフォルト: 5回）
    :param retry_delay: リトライ間の待機時間（秒）（デフォルト: 5秒）
    :return: 成功時はresponseオブジェクト、失敗時はNone
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(warc_url, stream=True)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                raise Exception(f"Invalid WARC URL: {warc_url}")
            else:
                print(f"{warc_url}: Got response.status_code == {response.status_code}. Retrying...")
        except ConnectionError as e:
            print(f"Connection error: {e}")

        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    print(f"Failed to download WARC file after {max_retries} attempts. Aborting.")
    return None


# メインの処理開始
# warcファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
#     処理手順:
#     1. warcファイルをダウンロード。（メモリ上に）
#     2. ダウンロードしたファイルをメモリ上に解凍
#     3. 解凍したデータをイテレートする
#     4. 日本語を対象として配列に追加
try:
    iteration = 0
    metadata_parser = XMLMetadataParser()
    lang_predictor = FastTextLangPredictor()
    refined_warc = []
    clear_tmp_file(temp_file_path)
    with tqdm(total=len(cleaned_warcs), unit='file', unit_scale=True, position=0) as pbar:
        for i, warc_path in enumerate(cleaned_warcs, 1):
            refined_warc = []

            # WARCファイルのURLを構築
            warc_url = f"https://data.commoncrawl.org/{warc_path}"

            # WARCファイルをダウンロード
            response = download_warc_file(warc_url)

            content = bytearray()
            is_response_accepted = False
            tmp_result = None
            for record in tqdm(ArchiveIterator(response.raw), position=1):
                if record.rec_type == 'response' and record.http_headers.get_header('Content-Type') == 'text/html':
                    content = record.content_stream().read()

                    lang = lang_detect(content, metadata_parser, lang_predictor)

                    if lang and lang[0][0] == "ja":
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

                        result["languages-fasttext"] = lang[0]
                        result["rec_headers"] = dict(record.rec_headers.headers)

                        # 辞書に情報を追加
                        tmp_result = result
                        is_response_accepted = True

                elif is_response_accepted and record.rec_type == 'metadata':
                    metadata = parse_metadata(record.content_stream().read())
                    if "languages-cld2" not in metadata or "languages" not in metadata["languages-cld2"]:
                        is_response_accepted = False
                        continue

                    languages = metadata["languages-cld2"]["languages"]
                    max_lang_code = max(languages, key=lambda x: x['text-covered'])['code']
                    if max_lang_code == "ja":
                        tmp_result["metadata"] = metadata
                        refined_warc.append(tmp_result)
                        is_response_accepted = False

            # 処理したデータを格納する配列に追加
            processed_file_names.add(warc_path)
            save_refined(refined_warc, temp_file_path)

            if i % zstd_chunk_size == 0:
                compress(temp_file_path, output_folder_name)
                clear_tmp_file(temp_file_path)

            # リソースの解放（念のため）
            del response
            pbar.update()

except Exception as e:
    traceback.print_exc()
finally:
    if get_file_size(temp_file_path) > 0:
        compress(temp_file_path, output_folder_name)

    clear_tmp_file(temp_file_path, create_empty=False)

    # 進捗データの保存
    progression = {"last_itr_count": max(iteration, last_itr_count), "processed_file_names": list(processed_file_names)}
    with open("progress.txt", "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
