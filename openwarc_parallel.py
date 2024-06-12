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

import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module='bs4')
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning, module='bs4')
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module='bs4')
warnings.filterwarnings("ignore", module='bs4')

hf_token = os.environ.get("HF_TOKEN", "")

parser = argparse.ArgumentParser(description='Process WARC files.')
parser.add_argument('--working_dir', type=str, help='Path to the working_dir.')
parser.add_argument('--dataset_dir', type=str, help='Path to the load and save dataset (not common crawl warc.')
parser.add_argument('--num_proc', type=int, default=8, help='Path to the load and save dataset (not common crawl warc.')
args = parser.parse_args()
working_dir = args.working_dir

dataset_dir = args.dataset_dir

# データの格納リスト

try:
    ja_soup_list = load_from_disk(dataset_dir).to_list()
except Exception:
    ja_soup_list = []

with open(os.path.join(working_dir, "data/202404/warc.paths"), "r", encoding="utf-8") as f:
    warc_paths = f.read().splitlines()

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

cleaned_warcs = []
for warc_path in warc_paths:
    if warc_path not in processed_file_names:
        cleaned_warcs.append(warc_path)


def process_warc(warc_path, last_itr_count):
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

        iteration = 0
        for record in ArchiveIterator(io.BytesIO(warc_data)):
            iteration += 1
            if iteration <= last_itr_count:
                last_itr_count = 0
                continue
            if record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    content = record.content_stream().read()
                    soup = BeautifulSoup(content, 'html.parser')

                    html_tag = soup.find('html')
                    if html_tag and html_tag.has_attr('lang'):
                        lang = html_tag['lang']
                        if lang == "ja":
                            json_data = extract(content, output_format='json', target_language="ja",
                                                deduplicate=True,
                                                include_formatting=True, include_tables=True)
                            if json_data == None:
                                continue
                            result = json.loads(json_data)

                            rejected = False
                            rejected_reason = ""
                            if len(result["text"]) < 400:
                                rejected = True
                                rejected_reason = "Too_Short"
                                continue

                            result["rejected"] = rejected
                            result["rejected_reason"] = rejected_reason
                            result["language"] = "ja"
                            result["commoncrawl_id"] = 0
                            result["url"] = record.rec_headers.get_header('WARC-Target-URI')
                            ja_soup_list.append(result)
                            # print(f"Found Japanese: \n\tURL: {result['url']}\n\tTitle: {result['title']}")

        del response
        del warc_data

        return True, warc_path, None, ja_soup_list
    except ParserRejectedMarkup as e:
        return False, warc_path, last_itr_count, ja_soup_list
    except Exception as e:
        traceback.print_exc()
        return False, warc_path, last_itr_count, ja_soup_list


results = None
total_iterations = len(cleaned_warcs)

def signal_handler(sig, frame):
    print('Ctrl+C pressed. Shutting down gracefully...')

    if results != None and len(results) > 0:
        print("Saving...")

        # 結果を結合
        for result in results:
            ja_soup_list.extend(result[3])
            if result[0]:
                processed_file_names.append(result[1])
                if result[1] in last_itr_counts:
                    del last_itr_counts[result[1]]
            else:
                last_itr_counts[result[1]] = result[2]

        dataset = Dataset.from_list(ja_soup_list)
        dataset.save_to_disk(dataset_dir)

        progression = {"last_itr_counts": last_itr_counts, "processed_file_names": processed_file_names}
        with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
            json.dump(progression, f, ensure_ascii=False)

    executor.shutdown(wait=True)

try:
    iteration = 0
    warc_with_info = []

    for warc_path in cleaned_warcs:
        last_itr_count = last_itr_counts[warc_path] if warc_path in last_itr_counts else 0
        warc_with_info.append((warc_path, last_itr_count))

    with tqdm(total=total_iterations, unit='file', unit_scale=True) as pbar:
        with ProcessPoolExecutor(max_workers=args.num_proc) as executor:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            try:
                futures = {executor.submit(process_warc, warc_path, last_itr_count): (warc_path, last_itr_count)
                           for warc_path, last_itr_count in warc_with_info}
                results = []
                for future in concurrent.futures.as_completed(futures):
                    warc_path, last_itr_count = futures[future]
                    result = future.result()
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
            processed_file_names.append(result[1])
            if result[1] in last_itr_counts:
                del last_itr_counts[result[1]]
        else:
            last_itr_counts[result[1]] = result[2]

    dataset = Dataset.from_list(ja_soup_list)
    print("Saving...")
    dataset.save_to_disk(dataset_dir)

    progression = {"last_itr_counts": last_itr_counts, "processed_file_names": processed_file_names}
    with open(os.path.join(working_dir, "progress_parallel.txt"), "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
