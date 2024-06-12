# Refined Common Crawl (ReCC)

FineWebとSwallowを参考にCommonCrawlから高品質な事前・継続学習用コーパスを作成するプロジェクト

- [ダウンロード用スクリプト](#ダウンロード用スクリプト)

## ダウンロード用スクリプト

スクリプトは以下の2種類。

- openwarc.py
- openwarc_parallel.py

openwarc.pyは並列処理なし、openwarc_parallel.pyはProcessPoolExecutorによる並列化あり  
PCごと落ちないようにopenwarc_parallel.pyはDockerで動かす想定

### 前準備

1. https://commoncrawl.org/get-started からCC-MAIN-2024-18を選択
2. warc.paths.gzをダウンロード
3. 解凍したものをdata/202404に配置

```
repository_root/
├─ data/
   ├─ 202404/
      ├─ warc.paths
```

#### openwarc_parallel.pyをDockerで使う場合

```
docker compose build
```

### 実行方法

#### openwarc.py

```
python openwarc.py
```

#### openwarc_parallel.py

```
docker compose up
```
または
```
python openwarc_parallel.py --working_dir={project_root} --dataset_dir={path_to_dataset} \
    --num_proc={max_workers, default=8}
```

### 具体的な処理

1. `working_dir/data/202404/warc.paths`からCommonCrawlのセグメントデータをダウンロードするurlを取得
2. CommonCrawlのセグメントデータをメモリ上にダウンロード
3. ダウンロードしたデータをメモリ上で展開
4. 展開したデータをArchiveIteratorを用いてイテレート
5. もしhtmlタグにlang=jaがあったら日本語と判断（Swallowより, beautifulsoup4使用）
6. 日本語のページはtrafilaturaを用いてMarkdown形式のテキスト情報を抽出（Swallowより）
7. `Ctrl+Cで処理が中断される`, `なんらかの致命的なエラーが出る`, `全てのセグメントの処理が終わる`のいずれかを満たすと`dataset_dir`にデータセットをArrowで保存する

並列処理は2, 3, 4, 5, 6で行われ、max_workers分だけ同時実行
