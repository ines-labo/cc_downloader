import argparse
import csv
import base64
import concurrent
import gzip
import json
import logging
import os
import shutil
import signal
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from json import JSONDecodeError

import boto3
import requests
import yaml
import zstandard
from timeout_timer import timeout, TimeoutInterrupt
from tqdm import tqdm
from trafilatura import extract
from ulid import ULID
from warcio.archiveiterator import ArchiveIterator
from multiprocessing import freeze_support

from lang_predictor import FastTextLangPredictor
from xml_parser import XMLMetadataParser


# config.yamlから設定を読み込む関数
def load_config(config_path='./config.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


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
            if value.startswith('{') and value.endswith('}'):
                metadata[key] = json.loads(value)
            else:
                metadata[key] = value

    return metadata


def download_warc_file(s3_client, bucket_name, warc_key, max_retries=5, retry_delay=5):
    """
    S3からWARCファイルをダウンロードする関数

    :param s3_client: boto3 S3クライアント
    :param bucket_name: S3バケット名
    :param warc_key: WARCファイルのS3キー
    :param max_retries: 最大リトライ回数（デフォルト: 5回）
    :param retry_delay: リトライ間の待機時間（秒）（デフォルト: 5秒）
    :return: 成功時はS3オブジェクト、失敗時はNone
    """
    for attempt in range(max_retries):
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=warc_key)
            return response['Body']
        except s3_client.exceptions.NoSuchKey:
            raise Exception(f"Invalid WARC key: {warc_key}")
        except Exception as e:
            print(f"{warc_key}: Exception {e}. Retrying...")

        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    print(f"Failed to download WARC file after {max_retries} attempts. Aborting.")
    return None


def read_csv_to_dict(file_path):
    result = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)  # ヘッダー行を読み込む
        for row in csv_reader:
            for i, column in enumerate(row):
                result[headers[i]] = column
    return result


def process_warc(bucket_name, warc_key, credential, use_fast_text=True, trafilatura_timeout=30, current_trial=0, max_trial=5, enable_text_extraction_from_html=True):
    """
    S3からWARCファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
    """
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

    def extract_data(content):
        return extract(content, output_format='json', target_language="ja",
                       deduplicate=True,
                       include_formatting=True, include_tables=True)

    # S3クライアントの初期化
    s3_client = boto3.client('s3', aws_access_key_id=credential["Access key ID"],
                             aws_secret_access_key=credential["Secret access key"],
                             region_name='us-east-1')

    print(f"Start: {warc_key}")
    result_list = []

    try:
        compressor = zstandard.ZstdCompressor()

        metadata_parser = None
        lang_predictor = None
        if use_fast_text:
            # xmlからメタデータをパースするやつ
            metadata_parser = XMLMetadataParser()
            # fasttextを使用して言語判定するやつ
            lang_predictor = FastTextLangPredictor()

        # S3からWARCファイルをダウンロード
        warc_object = download_warc_file(s3_client, bucket_name, warc_key)
        if warc_object is None:
            return False, warc_key, result_list

        tmp_content = None
        for record in ArchiveIterator(warc_object):
            if record.rec_type == 'response' and record.http_headers.get_header('Content-Type') == 'text/html':
                tmp_content = record.content_stream().read()

            elif record.rec_type == 'metadata':
                if tmp_content is None:
                    continue

                # メタデータのパース
                metadata = parse_metadata(record.content_stream().read())

                # cld2の解析が失敗 or languagesが存在しない場合スキップ
                if "languages-cld2" not in metadata or "languages" not in metadata["languages-cld2"]:
                    continue

                # 「日本語が最も多くを占めるページ」ではない場合スキップ
                languages = metadata["languages-cld2"]["languages"]
                max_lang_code = max(languages, key=lambda x: x['text-covered'])['code']
                if max_lang_code != "ja":
                    continue

                lang_fast_text = None
                if use_fast_text:
                    lang_fast_text = lang_detect(tmp_content, metadata_parser, lang_predictor)

                    # FastTextを使用している場合、日本語が検出されなかったらスキップ
                    if lang_fast_text is None or lang_fast_text[0][0] != "ja":
                        continue

                if enable_text_extraction_from_html:
                    # 本文の抽出にはtrafilaturaを用いる。（抽出精度が高いため）
                    # include_formatting=Trueにすることで、抽出したテキストがMarkdown形式になる（h2タグが見出しになったり、テーブルがパースされたり）
                    # deduplicateの効果は不明
                    try:
                        with timeout(trafilatura_timeout, timer="thread"):
                            json_data = extract_data(tmp_content)
                            result = json.loads(json_data)

                            # （Swallowより）本文の文字数が400以下の場合は低品質とみなす（ただしスキップはしない）
                            if len(result["text"]) < 400:
                                result["rejected"] = True
                                result["rejected_reason"] = "Too_Short"
                            else:
                                result["rejected"] = False
                                result["rejected_reason"] = ""

                            # FastTextの結果を格納
                            result["languages-fasttext"] = lang_fast_text[0] if lang_fast_text else None
                    except TimeoutInterrupt:
                        print(f"{warc_path}: Timeout triggered.")
                        compressed = compressor.compress(tmp_content)
                        result = {"raw_data": base64.b64encode(compressed).decode('utf-8'),
                                  "encoding": "base64",
                                  "timeout": True,
                                  "timeout_secs": trafilatura_timeout}
                    except:
                        continue
                else:
                    compressed = compressor.compress(tmp_content)
                    result = {"raw_data": base64.b64encode(compressed).decode('utf-8'), "encoding": "base64"}

                result["rec_headers"] = dict(record.rec_headers.headers)
                result["metadata"] = metadata

                result_list.append(result)
                tmp_content = None

        return True, warc_key, result_list
    except Exception as e:
        traceback.print_exc()
        if current_trial > max_trial:
            return False, warc_key, result_list
        print(f"{warc_key} restart the process.")
        del lang_predictor

        return process_warc(
            bucket_name=bucket_name,
            warc_key=warc_key,
            credential=credential,
            use_fast_text=use_fast_text,
            trafilatura_timeout=trafilatura_timeout,
            current_trial=current_trial+1,
            max_trial=max_trial,
            enable_text_extraction_from_html=enable_text_extraction_from_html
        )


def signal_handler(sig, frame):
    """
    SIGINTやSIGTERMが実行されたときに安全にデータを保存して複数プロセスで行っている処理をシャットダウンする。
    これが無いとPCごと逝く

    :param sig:
    :param frame:
    :return:
    """
    print('Ctrl+C pressed. Shutting down gracefully...')

    if get_file_size(temp_file_path) > 0:
        compress(temp_file_path, output_folder_path)

    clear_tmp_file(temp_file_path, create_empty=False)

    # 進捗データの保存
    progression = {"processed_file_names": processed_file_names}
    with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)

    print("Progression saved.")

    # ProcessPoolExecutorによる処理を中断
    executor.shutdown(wait=True)

    print("Executor shutdowned.")
    sys.exit(0)


def get_file_size(path):
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return 0


def save_refined(refined_data, path):
    if len(refined_data) == 0:
        return
    mode = "a"
    if not os.path.exists(path):
        mode = "w"
    # 一時ファイルにJSONLとして書き込む
    with open(path, mode, encoding="utf-8") as f:
        for item in refined_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    del refined_data


def clear_tmp_file(path, create_empty=True):
    try:
        if os.path.exists(path):
            os.remove(path)
        if create_empty:
            open(path, "w", encoding="utf-8").close()
    except Exception:
        traceback.print_exc()


def compress(src_path, output_folder_path):
    print("compressing and writing shards.")
    os.makedirs(output_folder_path, exist_ok=True)
    output_file_name = os.path.join(output_folder_path, str(ULID()) + ".zst")
    with open(src_path, "r", encoding="utf-8") as src_f, open(output_file_name, "wb") as out_f:
        cctx = zstandard.ZstdCompressor()
        with cctx.stream_writer(out_f) as compressor:
            for line in src_f:
                compressor.write(line.encode("utf-8"))
            compressor.flush()
    print("Compressed and saved to", output_file_name)


if __name__ == '__main__':
    freeze_support()

    # 実行時引数の設定
    parser = argparse.ArgumentParser(description='Process WARC files.')
    parser.add_argument('--config', type=str, default='./config.yaml', help='Path to the config file')
    args = parser.parse_args()

    # 設定を読み込む
    config = load_config(args.config)

    working_dir = config.get('working_dir')
    output_folder_path = config.get('dataset_dir')
    num_proc = config.get('num_proc')
    zstd_chunk_size = config.get('num_zstd_chunk_size')
    temp_file_path = config.get('temp_file_path')
    warc_paths_url = config.get('warc_paths_url')
    s3_credential = read_csv_to_dict(config.get('s3_credential'))
    use_fast_text = config.get('fast_text_language_recognition')
    trafilatura_timeout = config.get('trafilatura_timeout')
    enable_text_extraction_from_html = config.get('enable_text_extraction_from_html')

    # 実行時引数の値をprintで出力
    print(f"Working directory: {working_dir}")
    print(f"Dataset directory: {output_folder_path}")
    print("Note: If you are using Docker, these paths are within the container where this program is running :)")
    print(f"Number of processes: {num_proc}")
    print(f"Number of ZSTD chunk size: {zstd_chunk_size}")
    print(f"Use fast text for language recognition: {use_fast_text}")
    print(f"Trafilatura text extracting: {enable_text_extraction_from_html}")
    print(f"\tTimeout after: {trafilatura_timeout} secs")

    # trafilaturaによるwarningを抑制
    logging.getLogger("trafilatura.utils").setLevel(logging.ERROR)
    logging.getLogger("trafilatura.core").setLevel(logging.ERROR)

    # 前回実行時、処理が途中で中断された場合にデータセットと進捗を復元する
    # 1. warc.pathsファイルの読み込み
    # 2. 進捗の読み込み

    # warc.pathsファイルの読み込み
    # gzipファイルのダウンロード
    gzip_file_path = "./warc.paths.gz"
    response = requests.get(warc_paths_url)
    with open(gzip_file_path, "wb") as f:
        f.write(response.content)

    # gzipファイルの展開
    extracted_file_path = "./warc.paths"
    with gzip.open(gzip_file_path, 'rb') as f_in:
        with open(extracted_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # 展開したファイルの読み込み
    with open(extracted_file_path, "r", encoding="utf-8") as f:
        warc_paths = f.read().splitlines()

    # ダウンロードしたgzipファイルの削除（オプション）
    os.remove(gzip_file_path)
    os.remove(extracted_file_path)

    # 進捗の読み込み
    # 進捗データは処理済みセグメントファイル名の配列
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

    try:
        # 進捗バー表示のための全体のデータ数
        total_iterations = len(cleaned_warcs)
        # 一時ファイルの初期化
        clear_tmp_file(temp_file_path)
        # 並列処理の実行
        with tqdm(total=total_iterations, unit='file', unit_scale=True) as pbar:
            with ProcessPoolExecutor(max_workers=num_proc) as executor:
                # InterruptとTerminateのハンドラを設定
                signal.signal(signal.SIGINT, signal_handler)
                signal.signal(signal.SIGTERM, signal_handler)
                try:
                    def on_process_finished(future):
                        pbar.update(1)
                        result = future.result()  # ここでのresultは(bool, str, list[dict])
                        if result[0]:
                            # 一時ファイルに保存
                            save_refined(result[2], temp_file_path)
                            # もし処理したファイル数がchunk sizeになったらzstd圧縮して保存
                            if pbar.n % zstd_chunk_size == 0:
                                compress(temp_file_path, output_folder_path)
                                clear_tmp_file(temp_file_path)
                            # 処理済みファイル名を追加
                            processed_file_names.append(result[1])

                    for warc_path in cleaned_warcs:
                        future = executor.submit(process_warc, "commoncrawl", warc_path, s3_credential, use_fast_text, trafilatura_timeout, 0, 5, enable_text_extraction_from_html)
                        future.add_done_callback(on_process_finished)
                except:
                    traceback.print_exc()

    except Exception as e:
        traceback.print_exc()
    finally:
        print("finishing main roop...")
        if get_file_size(temp_file_path) > 0:
            compress(temp_file_path, output_folder_path)

            clear_tmp_file(temp_file_path, create_empty=False)

            # 進捗データの保存
            progression = {"processed_file_names": processed_file_names}
            with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
                json.dump(progression, f, ensure_ascii=False)
