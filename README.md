# Refined Common Crawl (ReCC)

FineWebとSwallowを参考にCommonCrawlから高品質な事前・継続学習用コーパスを作成するプロジェクト

- [ダウンロード用スクリプト](#ダウンロード用スクリプト)

## ダウンロード用スクリプト

スクリプトは以下の2種類。

- ~~openwarc.py~~ (deprecated, メンテナンスされてない)
- openwarc_parallel.py

openwarc.pyは並列処理なし、openwarc_parallel.pyはProcessPoolExecutorによる並列化あり  
openwarc_parallel.pyはDockerで動かす想定

### 前準備

1. https://commoncrawl.org/get-started から欲しいスナップショットのwarc.pathsのダウンロードURLを取得
2. [設定ファイル](#設定ファイル)を見ながらconfig.yamlを書く
2. warc.paths.gzをダウンロード
3. 解凍したものをdata/202404に配置
4. `download_weights.py`を実行（fasttextのウェイトファイルをダウンロードする）（dockerを使用する場合は既にDockerfileに書いてあるので実行する必要は無い）

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

### 設定ファイル

実行時引数が多くなってきたためyaml形式の設定ファイルを読み込むようにした
リポジトリに予め含まれているconfig.yamlを参考にしてほしい
デフォルトだとこんな感じ↓

```yaml
working_dir: /mnt
dataset_dir: /mnt/dataset
num_proc: 8
num_zstd_chunk_size: 1000
temp_file_path: ./temp_refined_warc_samples.jsonl
warc_paths_url: https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-18/warc.paths.gz
s3_credential: /mnt/s3_accessKeys.csv
fast_text_language_recognition: False
enable_text_extraction_from_html: False
trafilatura_timeout: 30
```

| 引数名                           | 説明                                                                           |
|-------------------------------|------------------------------------------------------------------------------|
| working_dir                   | 進捗状況を保存するファイルが置かれるフォルダ                                                       |
| dataset_dir                   | 抽出された圧縮済みデータの保存先フォルダ                                                         |
| num_proc                      | 並列実行するプロセス数。                                                                 |
| num_zstd_chunk_size           | この数のwarcファイルを処理した後にzstd圧縮したデータが保存される。                                        |
| temp_file_path                | 一時ファイルの保存先（ファイル名）                                                            |
| warc_paths_url                | warc.paths.gzのダウンロード先URL                                                     |
| s3_credential                 | AWS S3のクレデンシャルcsvファイルのパス。<br/>`Access key ID`と`Secret access key`が保存されているはず。 |
| fast_text_language_recognition | FastTextによる言語判定を利用するかどうか。                                                    |
| enable_text_extraction_from_html | TrafilaturaによるHTMLからのテキスト抽出を行うかどうか。                                         |
| trafilatura_timeout             | Trafilaturaのテキスト抽出にこの秒数以上必要とする場合、処理を中断し、テキストではなくbase64エンコーディングされたデータを格納します。 |

### 実行方法

#### ~~openwarc.py~~ (Deprecated)

```
python openwarc.py
```

#### openwarc_parallel.py

```
docker compose up -d
```
または
```
python openwarc_parallel.py --config=/path/to/config.yaml
```

### 具体的な処理

1. `working_dir/data/202404/warc.paths`からCommonCrawlのセグメントデータをダウンロードするurlを取得
2. CommonCrawlのwarcファイルをストリーミングしながらArchiveIteratorを用いてイテレート
5. `（CommonCrawlのcld2で最もtext-covered率が高い言語が日本語）== True`でフィルタリング
6. もしFastTextを使用するなら`（FastTextによる言語判定で日本語が出る） == True`でフィルタリング
6. 日本語のページはtrafilaturaを用いてMarkdown形式のテキスト情報を抽出（Swallowより）
7. `Ctrl+Cで処理が中断される`, `なんらかの致命的なエラーが出る`, `処理したwarcファイルが指定されたチャンクサイズを超える`, `全てのセグメントの処理が終わる`のいずれかを満たすと`dataset_dir`に<u>データをzstd圧縮して保存する。なおulidで命名</u>

並列処理は2, 3, 4, 5, 6で行われ、max_workers分だけ同時実行

### データ形式

基本trafilaturaそのままだが、フィルタリングによって弾かれた内容についてのフィールドが追加されている。

| フィールド名             | 型    | 説明                                                  |
|--------------------|------|-----------------------------------------------------|
| rejected           | bool | この文書がフィルタリングによって破棄されたかどうか。破棄された場合True、去れなかった場合False |
| rejected_reason    | str  | 破棄された場合の理由。破棄されなかった場合は空文字                           |
| languages-fasttext | dict | FastTextによる言語解析の結果                                  |
| rec_headers        | dict | Common Crawlのリクエストヘッダー                              |
| metadata           | dict | Common Crawlがこのエントリに対して付与したメタデータ                    |


```
{
  "title": "『ペルソナ５ ザ・ロイヤル』公式美術設定集が本日より発売開始！ | ペルソナチャンネル | ペルソナシリーズ最新情報",
  "author": null,
  "hostname": "p-ch.jp",
  "date": "2024-04-01",
  "fingerprint": "3f4dfe12a80448d7",
  "id": null,
  "license": null,
  "comments": "",
  "raw_text": "2024.03.28  『ペルソナ５ ザ・ロイヤル』公式美術設定集が本日より発売開始！  一昨年、マルチハードでリマスター版も発売され、ますますたくさんの方にお楽しみいただいている 『ペルソナ５ ザ・ロイヤル』の公式美術設定集が本日より発売開始 いたしました！ \n『ペルソナ５』はいかにして『ペルソナ５ ザ・ロイヤル』へと“深化”したのか？ \n前著 『ペルソナ５ 公式設定画集』と同様にアートワークが網羅されているのはもちろん、 今回は新たな試みとして3Dモデルにも注目！3Dモデルの一部も収録 されています！ さらに、 アート・３D両方の製作スタッフの声に加え、開発時のエピソードを聞いたロングインタビューも2本掲載！ ※画像はクリックまたはタップで拡大します。  『ペルソナ』シリーズを生み出しているP-STUDIOのクリエイティブを美術の目線からまとめた決定版です！ \nさらに、エクストラコンテンツとして、主に『ペルソナ』シリーズ25周年期間となる2021～2022年に描かれた記念イラストやコラボイラスト類も収録しています。 通常版に加え、 「B2タペストリー」「ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット」 などオリジナル特典がついたebtenDXパックも、ebten内アトラスDショップ限定で発売中！ ebtenDXパック限定特典「B2タペストリー」、 \n「ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット」 『ペルソナ５ ザ・ロイヤル』のアートワークがぎゅっと詰まった一冊をぜひこの機会にお手元にどうぞ！  【商品情報】 書名： ペルソナ５ ザ・ロイヤル 公式美術設定集 発売日： 2024年3月28日（木） 仕様： オールカラー／A4判／本体272ページ 定価： 3,520円［税込］ 発行： 株式会社KADOKAWA Game Linkage 発売： 株式会社KADOKAWA 【ebtenDXパック 商品情報】 書名： ペルソナ５ ザ・ロイヤル 公式美術設定集 ebtenDXパック 発売日： 2024年3月28日（木） 仕様： オールカラー／A4判／本体272ページ 価格： 6,270円（書籍：3,520円+グッズ：2,750円）［税込］ 発行： 株式会社KADOKAWA Game Linkage 発売： 株式会社KADOKAWA 同梱物： ■B2タペストリー ■ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット ※ECサイト[ebten]専売",
  "text": "2024.03.28\n『ペルソナ５ ザ・ロイヤル』公式美術設定集が本日より発売開始！\n一昨年、マルチハードでリマスター版も発売され、ますますたくさんの方にお楽しみいただいている『ペルソナ５ ザ・ロイヤル』の公式美術設定集が本日より発売開始いたしました！\n『ペルソナ５』はいかにして『ペルソナ５ ザ・ロイヤル』へと“深化”したのか？\n前著『ペルソナ５ 公式設定画集』と同様にアートワークが網羅されているのはもちろん、今回は新たな試みとして3Dモデルにも注目！3Dモデルの一部も収録されています！\nさらに、アート・３D両方の製作スタッフの声に加え、開発時のエピソードを聞いたロングインタビューも2本掲載！\n※画像はクリックまたはタップで拡大します。\n『ペルソナ』シリーズを生み出しているP-STUDIOのクリエイティブを美術の目線からまとめた決定版です！\nさらに、エクストラコンテンツとして、主に『ペルソナ』シリーズ25周年期間となる2021～2022年に描かれた記念イラストやコラボイラスト類も収録しています。\n通常版に加え、「B2タペストリー」「ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット」などオリジナル特典がついたebtenDXパックも、ebten内アトラスDショップ限定で発売中！\nebtenDXパック限定特典「B2タペストリー」、\n「ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット」\n『ペルソナ５ ザ・ロイヤル』のアートワークがぎゅっと詰まった一冊をぜひこの機会にお手元にどうぞ！\n【商品情報】\n書名：ペルソナ５ ザ・ロイヤル 公式美術設定集\n発売日：2024年3月28日（木）\n仕様：オールカラー／A4判／本体272ページ\n定価：3,520円［税込］\n発行：株式会社KADOKAWA Game Linkage\n発売：株式会社KADOKAWA\n【ebtenDXパック 商品情報】\n書名：ペルソナ５ ザ・ロイヤル 公式美術設定集 ebtenDXパック\n発売日：2024年3月28日（木）\n仕様：オールカラー／A4判／本体272ページ\n価格：6,270円（書籍：3,520円+グッズ：2,750円）［税込］\n発行：株式会社KADOKAWA Game Linkage\n発売：株式会社KADOKAWA\n同梱物：\n■B2タペストリー\n■ペルソナ５ ザ・ロイヤル Picaresque Mouse 缶バッジ ヴァイオレット\n※ECサイト[ebten]専売",
  "language": null,
  "image": "https://p-ch.jp/wp-content/uploads/2024/03/20240312_img02.jpg",
  "pagetype": "article",
  "filedate": "2024-07-08",
  "source": "https://p-ch.jp/news/14085/",
  "source-hostname": "ペルソナチャンネル | ペルソナシリーズ最新情報",
  "excerpt": "こんにちは、ペルソナ広報です。一昨年、マルチハードでリマスター版も発売され、ますますたくさんの方にお楽しみいただいている『ペルソナ５ ザ・ロイヤル』の公式美術設定集が本日より発売開始いたしました！『ペ...",
  "categories": "",
  "tags": "ペルソナ,Persona,ペルソナチャンネル,PERSONA CHANNEL,ペルソナ３,ペルソナ４,ペルソナ最新作,P3,P4,P4G,P3P,ペルソナQ シャドウ オブ ザ ラビリンス,PQ,ペルソナ4 ダンシング・オールナイト,P4D,ペルソナ4 ジ・アルティマックス ウルトラスープレックスホールド,P4U2,ペルソナ５,P5,ペルソナ3 ダンシング・ムーンナイト,ペルソナ5 ダンシング・スターナイト,P3D,P5D",
  "rejected": false,
  "rejected_reason": "",
  "languages-fasttext": null,
  "rec_headers": {
    "WARC-Type": "metadata",
    "WARC-Date": "2024-04-13T12:13:33Z",
    "WARC-Record-ID": "<urn:uuid:7e5ddd4d-da3a-4429-bd50-dd28e26f06af>",
    "Content-Length": "289",
    "Content-Type": "application/warc-fields",
    "WARC-Warcinfo-ID": "<urn:uuid:74abb668-9ce7-49f2-b526-91e3b5dd3664>",
    "WARC-Concurrent-To": "<urn:uuid:fa95d872-d319-4f50-909f-e3455781b6ee>",
    "WARC-Target-URI": "http://p-ch.jp/news/14085/"
  },
  "metadata": {
    "fetchTimeMs": 396,
    "charset-detected": "UTF-8",
    "languages-cld2": {
      "reliable": true,
      "text-bytes": 3460,
      "languages": [
        {
          "code": "ja",
          "code-iso-639-3": "jpn",
          "text-covered": 0.8,
          "score": 3398.0,
          "name": "Japanese"
        },
        {
          "code": "en",
          "code-iso-639-3": "eng",
          "text-covered": 0.12,
          "score": 899.0,
          "name": "ENGLISH"
        }
      ]
    }
  }
}
```
