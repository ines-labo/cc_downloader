import gzip
import io
import json
import traceback

import requests
from bs4 import BeautifulSoup
from datasets import Dataset, load_from_disk
from warcio.archiveiterator import ArchiveIterator
from trafilatura import fetch_url, extract, extract_metadata
from tqdm import tqdm

# TBD
warc_file = 'data/202404/CC-MAIN-20240412101354-20240412131354-00012.warc'
# データセットを格納する場所
output_file = "/mnt/nvme2n1/dataset/commoncrawl"

# 前回実行時、処理が途中で中断された場合にデータセットと進捗を復元する
# 1. データセットのロード
# 2. warc.pathsファイルの読み込み
# 3. 進捗の読み込み

# データセットのロード
try:
    ja_soup_list = load_from_disk(output_file).to_list()
except Exception:
    ja_soup_list = []

# 途中から再開する用の位置情報の取得
if len(ja_soup_list) > 0:
    final_id = ja_soup_list[-1]["commoncrawl_id"]
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

# メインの処理開始
# warcファイルを読み込んで、日本語ページかどうかの簡単なフィルタリングを行う。
#     処理手順:
#     1. warcファイルをダウンロード。（メモリ上に）
#     2. ダウンロードしたファイルをメモリ上に解凍
#     3. 解凍したデータをイテレートする
#     4. 日本語を対象として配列に追加
try:
    iteration = 0
    for warc_path in cleaned_warcs:
        print(f"Progress... {iteration + 1} / {len(warc_path)}")

        if warc_path in processed_file_names:
            print("Skipping.")
            continue

        # WARCファイルのURLを構築
        warc_url = f"https://data.commoncrawl.org/{warc_path}"

        # WARCファイルをダウンロード
        response = requests.get(warc_url, stream=True)
        total_size = int(response.headers.get("Content-Length", 0))
        block_size = 1024  # 1 KB

        with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {warc_path}") as pbar:
            content = bytearray()
            for data in response.iter_content(block_size):
                content.extend(data)
                pbar.update(len(data))

        # ダウンロードしたWARCファイルを解凍
        with tqdm(total=len(content), unit="B", unit_scale=True, desc=f"Decompressing {warc_path}") as pbar:
            decompressed_data = bytearray()
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz_file:
                warc_data = gz_file.read()
                pbar.update(len(content))

        # ArchiveIteratorでイテレートする
        iteration = 0
        for record in tqdm(ArchiveIterator(io.BytesIO(warc_data))):
            final_id += 1
            iteration += 1
            # 前回処理が終了した時点までスキップ
            if iteration <= last_itr_count:
                last_itr_count = 0
                continue

            # 正常に取得できていて、Content-Typeが"text/html"のものを対象とする
            if record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    content = record.content_stream().read()
                    try:
                        # HTMLタグのlang属性の値を取得するためにBeautifulSoupを使用
                        # lang="ja"なら日本語ページとみなす (Swallowにならって）
                        soup = BeautifulSoup(content, 'html.parser')
                    except Exception as e:
                        continue

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

                            # （Swallowより）本文の文字数が400以下の場合は低品質とみなしてスキップ
                            if len(result["text"]) < 400:
                                continue

                            # 辞書に情報を追加
                            result["language"] = "ja"
                            result["commoncrawl_id"] = final_id
                            result["url"] = record.rec_headers.get_header('WARC-Target-URI')
                            ja_soup_list.append(result)
                            print(f"Found Japanese: \n\tURL: {result['url']}\n\tTitle: {result['title']}")

        # 処理したデータを格納する配列に追加
        processed_file_names.add(warc_path)
        # リソースの解放（念のため）
        del response
        del warc_data

except Exception as e:
    traceback.print_exc()
finally:
    # 処理結果を保存
    dataset = Dataset.from_list(ja_soup_list)
    dataset.save_to_disk(output_file)

    # 進捗データの保存
    progression = {"last_itr_count": max(iteration, last_itr_count), "processed_file_names": list(processed_file_names)}
    with open("progress.txt", "w", encoding="utf-8") as f:
        json.dump(progression, f, ensure_ascii=False)
