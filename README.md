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

### データ形式

基本trafilaturaそのままだが、フィルタリングによって弾かれた内容についてのフィールドが追加されている。

| フィールド名          | 型    | 説明                                                  |
|-----------------|------|-----------------------------------------------------|
| rejected        | bool | この文書がフィルタリングによって破棄されたかどうか。破棄された場合True、去れなかった場合False |
| rejected_reason | str  | 破棄された場合の理由。破棄されなかった場合は空文字                           |
| commoncrawl_id  | int  | フィルタリングしたデータに対して連番にidを振る**予定**。現在は0で固定              |
| url             | str  | common crawlのクローラがダウンロードした生のURL。           |


```
{
  "title": "犬ブリーダーの探し方、子犬の見つけ方",
  "author": null,
  "hostname": "dogoo.com",
  "date": "1998-01-01",
  "fingerprint": "996b07f65c52c067",
  "id": null,
  "license": null,
  "comments": "",
  "raw_text": "犬ブリーダーの探し方、子犬の見つけ方 を教えてください。\n 【状況と今の対処】  \n今、新たに犬を飼おうと思っています。 \n犬の入手方法を探すと「子犬の買い方は、ペットショップよりブリーダーから直に買うのが良い」との意見を多く目にします。 \n \n直接ブリーダーから犬を買いたいのですが、良い犬の繁殖家ブリーダーは見つかりません。 \n良いブリーダーの探す方法や選び方、見つける方法はありませんか。 \n私は島根県に住んでます。知っている人がいたら教えて下さい。\n\n ブリーダーの探し方や見つけ方 ブリーダーから買った人 さん \nよいブリーダーの探し方で、 私がおススメする方法は、「自分が飼いたい犬種、欲しい犬をすでに飼っている人に聞く」 です。\n 【 ブリーダーの探し方や見つけ方 】 知人や、散歩している人に質問する インターネット、ブログやSNSで同じ犬種を飼っている人を探す、聞いてみる ドッグショーへ行って、質問する 動物病院で紹介してもらう ペット美容室（トリマー専門店）で紹介してもらう 犬の専門雑誌 \n恥ずかしい、照れくさい、変な人・面倒くさい人と思われたら・・・その気持ち分かります。 \n私は、可愛い犬を散歩している人に勇気を振り絞り、話しかけました。とても親切に教えていただき、ご紹介いただいたブリーダーさんで、子犬が産まれるのを待って購入しました。 \n \n一方で、友人は某SNSを利用し、数名の方へどこで子犬を購入したか、問い合せたそうです。 \nいきなり「子犬をどこで購入しましたか？」は失礼なので、SNSのフォロワーであること、どの写真やどの記事が良かったなど感想を入れ、そして同じ犬種を飼いたいことを書き、 犬への真剣さが伝われば、教えてくれる可能性 が高まります。　犬の犬舎ブリーダー 全国リスト - dogoo.com\n \n \nひと昔前は犬の雑誌に数多くのブリーダー広告がありましたが、今は広告も減りました。 \n雑誌には、ネットを使ってない熟練の専業ブリーダーさんの広告もあるので、ちらっと見ると参考になる情報があるかも。 \n動物病院やペット美容室は、ペットショップが兼業している店舗も多く「ペットショップで購入して」と返答されることも多いかと（苦笑）\n ブリーダーの種類タイプ、見分け方 トロント さん \n 良いブリーダーを探すのは、とても難しい です。 \nブリーダーが繁殖する子犬には、2つのタイプがあります。\n ドッグショー入賞を目指す 家庭犬、ペット用として、ペットショップに流通する \n同じ繁殖活動でも、良い犬の考え方、飼育方法も違います。 \nどちらが良いはなく、ドッグショーを目指すわけでもないのに、ドッグショータイプの犬が全ての方に良いとは限りません。 \n \n\nそして、１つの犬種にしぼり専門でブリーディングしている方、多犬種をブリーディングしている方、またはブリーダー業と名乗るが犬は飼育せず仲介手数料を生業とする方、いろいろなブリーダーがいます。 \n問合せや取引する ブリーダーがどのタイプなのか 、よく知ってからコンタクトをとりましょう。\n 【 ブリーダーの種類・タイプ 】 繁殖する犬のタイプ：ドッグショーを目指す or 家庭犬を繁殖する 犬種の数：単一犬種のみ取り扱い or 多頭犬種を管理・繁殖 子犬の仲介業、犬を飼育せず紹介専門、仲買い 犬種の流行を追い続け、流行の子犬を高く売るだけを目的にし劣悪な環境で繁殖する、 パピーミル（子犬工場） という俗語もあります。 \n \n飼育環境を確認するのに、犬舎の見学は大事ですが、真剣にブリーディング活動している方は、余計なウィルス感染を防ぐために 犬舎見学を断っている方もいます 。犬舎見学を断る理由は、犬舎を見学に来る方が、複数の犬舎を訪問しウィルスの媒介者となる恐れがあるからです。 \n \n良い子犬を提供しようと日夜努力しているブリーダーさんのなかには、犬を探す方からの「写真を送って」「値下げして」などの一言メッセージや無理な要望へ、親切な対応に疲れている方もいます。 \n \n良いブリーダーと一概にいっても、何を基準にするかで違います。 \n 犬種の特徴や注意点、遺伝的な疾患、飼い方など、犬種について情報を集める と、自分にあったブリーダーの基準がわかってくるかもしれません。　犬の飼い方 〜 初心者もわかる準備チェック・リスト ブリーダーを探す前に、犬種について勉強 メイ さん \n ブリーダーを探す前に、犬種について勉強が必要 です。 \n適正な体格、体高、体重の数値を知り、親犬の大きさやサイズを確認する。小さければ良いわけではありません。 \nレトリーバー系の股関節形成不全、プードルのパテラ（膝蓋骨脱臼）など、犬種特有の病気や疾患を知り、チェックする。 \n \n 犬種ごとに、被毛、目の色など遺伝疾患 と言われる特徴があります。（もちろん遺伝疾患を持っていても元気に育つ犬も少なくありません。） \n珍しい色と稀なカラーとして高値で売れる理由により、無理な繁殖はしていないか？ \n \n3代前までの血統書を見せてもらい、近親交配はないか、母犬の出産年齢は適齢か？ \n早すぎても遅すぎても良くないです。母犬の出産適齢期は2歳〜5歳と言われています。 \n \n最後に、子犬の売り文句である”チャンピオン直系”、”チャンピオン直子”という言葉に惑わされないように。 \n多くの純血種にとって、血筋をたどれば親族に何かしらのチャンピオンがいます（笑）\n 良いブリーダーの見つけ方 トイプードル２飼い さん \n取引するブリーダーさんは、法律で保健所に動物取扱業者番号の登録が義務化されています。 \n犬を探す人が、ブリーダーへ要望を出せば、動物取扱業者番号の詳細を見せてくれます。そこには、犬舎の名称、責任者名、登録日や有効期限の記載があります。 \n \n 良いブリーダーの見つけ方１つの例として、「登録日」に注視する と、いつからブリーダー業を営んでいるか分かります。 \n何度か更新し「有効期限の期日」が延長されても（有効期限は5年）、「登録日」は最初に登録した日が記載されます。 \n法律の施行は2006年6月からなので、それ以前の日付はありませんが、長年ブリーディングしている方なのか、登録して間もない新規参入したブリーダーなのか分かります。\n長く営業していれば必ず良いわけではありませんが、参考の１つになります。\n ブリーダー探し、愛知から長野まで うさうさ さん \nもし質問主さんがブリーダー探しに労を惜しまないのであれば、近県まで出向くのも仕方ないと思います。 \n希望する犬種の赤ちゃんが、すぐ近くで繁殖しタイミングよく産まれるなんて事は珍しいですからね〜＾＾； \nうちは 愛知県から長野県まで片道3時間以上掛けて、ブリーダーの元へ子犬 を見に行きました。 \n \nそれともう一つ、獣医さん、トリマーさんを介して紹介して頂くのもイイかと思います。 \nだいたいの上限価格を告げれば、多少時間が掛かるもののちゃんと対応して下さいますよ＾＾ \nただ、獣医さんから仲介して頂いた場合、親犬を見る事は出来ない事が多い様です。でも獣医さんがしっかり選んで下さるなら安心です＾＾　犬を飼う初期費用、1年間にかかる料金と必要リスト ブリーダーの法律、対面の義務化 ダックス さん \nブリーダーで子犬の購入は、法律により一昔前と違う点があります。 \nかつては、遠く離れたブリーダーと連絡をとり、代金を払えば、空輸や陸路で子犬を自宅まで届ける事業者もありました。 \n 2019年以降は、法律（改正動物愛護管理法）で禁止 です。 *1\n \n犬の購入希望者は、 ブリーダーが開業した事業所まで行き 、犬の現物確認と対面説明を受ける義務があります。また、生後56日以下の子犬について譲渡禁止です。 \n\n \n対面するため、時間とお金が必要になります。 \nそれでもブリーダーから、直接、犬を購入する方は少なくありません。 \n \nブリーダーの元へ車で数時間かける方もいれば、新幹線で子犬を迎えに行く方もいます。 \nまた、先に現物確認と対面説明を受けにブリーダーを訪問し、後日、飛行機で輸送する方もいます。　犬と飛行機で旅行、料金やケージ規定サイズ \n  *1 ・第一種動物取扱業者の規制 - 販売時の対面説明:動物を購入しようとする者に対して、事業所において、その動物の現状を直接見せる（現物確認）と共に、その動物の特徴や適切な飼養方法等について対面で文書（電磁的記録を含む）を用いて説明（対面説明）することが必要となります。 - ５６日齢以下の販売制限 引用元: 環境省 - 動物愛護管理法 販売数や口コミは？ 通りすがり さん \nネットで犬のブリーダーを紹介するサイトには、今までの 販売数（契約数、譲渡数）、口コミや評価 があります。 \n数値データやユーザーの投稿コメントで、ブリーダーを探す方も少なくないです \n \nただ、販売数が多ければ良いかと言うと、それだけ多くの犬種や頭数を仲介・販売する大手ペット事業者となります。 \n一方で、母犬に配慮したブリードする個人経営の犬舎さんは、年に数頭しか譲渡しないので、数値データは少なく、口コミ数も下位になります。 \nどちらが良いではなく、 数値データやコメント数のみで判断しない ように注意してください。 \n \nコメント内容は、本当に信用できるのか、苦情や批判の対応はどうか？ \nコメント数が多いのに、高評価のコメントばかり、悪いコメントが少ないのは、何かあるのでは？と少し疑問を持つ。 \n販売数や口コミは、とても有意義な情報です。が、全ての情報を鵜呑みにせず、参考程度が良いです。\n ブリーダー探し、新聞の譲ります ひ太 さん 探している犬種はビーグルかダックスでしたね。ブリーダーさん探しでは意外な方法がありますよ。 \n特に島根限定でお探しなら、 新聞の譲りますコーナーをまめにチェック するのも手ですよ。 \n中国地方では有名な某新聞の譲りますコーナーで、ペットのコーナーを探すとダックス、ビーグルだとか掲載されているのを見かけます。 \n \n但し、ペット情報は土日の新聞にまとめて載るみたいで、 \n平日のこともありますが、毎日載っていないので申し訳ないのだけれども・・・・それなりに県内のブリーダーさんの情報が載ると思います。 \n犬種限定だと、1ヶ月くらいはチェックが必要ですが、人気犬種は良く載ってますよ。",
  "text": "犬ブリーダーの探し方、子犬の見つけ方を教えてください。\n【状況と今の対処】\n今、新たに犬を飼おうと思っています。\n犬の入手方法を探すと「子犬の買い方は、ペットショップよりブリーダーから直に買うのが良い」との意見を多く目にします。\n直接ブリーダーから犬を買いたいのですが、良い犬の繁殖家ブリーダーは見つかりません。\n良いブリーダーの探す方法や選び方、見つける方法はありませんか。\n私は島根県に住んでます。知っている人がいたら教えて下さい。\nブリーダーの探し方や見つけ方\nブリーダーから買った人 さん\nよいブリーダーの探し方で、私がおススメする方法は、「自分が飼いたい犬種、欲しい犬をすでに飼っている人に聞く」です。\n【ブリーダーの探し方や見つけ方】\n- 知人や、散歩している人に質問する\n- インターネット、ブログやSNSで同じ犬種を飼っている人を探す、聞いてみる\n- ドッグショーへ行って、質問する\n- 動物病院で紹介してもらう\n- ペット美容室（トリマー専門店）で紹介してもらう\n- 犬の専門雑誌\n恥ずかしい、照れくさい、変な人・面倒くさい人と思われたら・・・その気持ち分かります。\n私は、可愛い犬を散歩している人に勇気を振り絞り、話しかけました。とても親切に教えていただき、ご紹介いただいたブリーダーさんで、子犬が産まれるのを待って購入しました。\n一方で、友人は某SNSを利用し、数名の方へどこで子犬を購入したか、問い合せたそうです。\nいきなり「子犬をどこで購入しましたか？」は失礼なので、SNSのフォロワーであること、どの写真やどの記事が良かったなど感想を入れ、そして同じ犬種を飼いたいことを書き、犬への真剣さが伝われば、教えてくれる可能性が高まります。 犬の犬舎ブリーダー 全国リスト - dogoo.com\nひと昔前は犬の雑誌に数多くのブリーダー広告がありましたが、今は広告も減りました。\n雑誌には、ネットを使ってない熟練の専業ブリーダーさんの広告もあるので、ちらっと見ると参考になる情報があるかも。\n動物病院やペット美容室は、ペットショップが兼業している店舗も多く「ペットショップで購入して」と返答されることも多いかと（苦笑）\nブリーダーの種類タイプ、見分け方\nトロント さん\n良いブリーダーを探すのは、とても難しいです。\nブリーダーが繁殖する子犬には、2つのタイプがあります。\n- ドッグショー入賞を目指す\n- 家庭犬、ペット用として、ペットショップに流通する\n同じ繁殖活動でも、良い犬の考え方、飼育方法も違います。\nどちらが良いはなく、ドッグショーを目指すわけでもないのに、ドッグショータイプの犬が全ての方に良いとは限りません。\nそして、１つの犬種にしぼり専門でブリーディングしている方、多犬種をブリーディングしている方、またはブリーダー業と名乗るが犬は飼育せず仲介手数料を生業とする方、いろいろなブリーダーがいます。\n問合せや取引するブリーダーがどのタイプなのか、よく知ってからコンタクトをとりましょう。\n【ブリーダーの種類・タイプ】\n- 繁殖する犬のタイプ：ドッグショーを目指す or 家庭犬を繁殖する\n- 犬種の数：単一犬種のみ取り扱い or 多頭犬種を管理・繁殖\n- 子犬の仲介業、犬を飼育せず紹介専門、仲買い\n犬種の流行を追い続け、流行の子犬を高く売るだけを目的にし劣悪な環境で繁殖する、パピーミル（子犬工場）という俗語もあります。\n飼育環境を確認するのに、犬舎の見学は大事ですが、真剣にブリーディング活動している方は、余計なウィルス感染を防ぐために犬舎見学を断っている方もいます。犬舎見学を断る理由は、犬舎を見学に来る方が、複数の犬舎を訪問しウィルスの媒介者となる恐れがあるからです。\n良い子犬を提供しようと日夜努力しているブリーダーさんのなかには、犬を探す方からの「写真を送って」「値下げして」などの一言メッセージや無理な要望へ、親切な対応に疲れている方もいます。\n良いブリーダーと一概にいっても、何を基準にするかで違います。\n犬種の特徴や注意点、遺伝的な疾患、飼い方など、犬種について情報を集めると、自分にあったブリーダーの基準がわかってくるかもしれません。 犬の飼い方 〜 初心者もわかる準備チェック・リスト\nブリーダーを探す前に、犬種について勉強\nメイ さん\nブリーダーを探す前に、犬種について勉強が必要です。\n適正な体格、体高、体重の数値を知り、親犬の大きさやサイズを確認する。小さければ良いわけではありません。\nレトリーバー系の股関節形成不全、プードルのパテラ（膝蓋骨脱臼）など、犬種特有の病気や疾患を知り、チェックする。\n犬種ごとに、被毛、目の色など遺伝疾患と言われる特徴があります。（もちろん遺伝疾患を持っていても元気に育つ犬も少なくありません。）\n珍しい色と稀なカラーとして高値で売れる理由により、無理な繁殖はしていないか？\n3代前までの血統書を見せてもらい、近親交配はないか、母犬の出産年齢は適齢か？\n早すぎても遅すぎても良くないです。母犬の出産適齢期は2歳〜5歳と言われています。\n最後に、子犬の売り文句である”チャンピオン直系”、”チャンピオン直子”という言葉に惑わされないように。\n多くの純血種にとって、血筋をたどれば親族に何かしらのチャンピオンがいます（笑）\n良いブリーダーの見つけ方\nトイプードル２飼い さん\n取引するブリーダーさんは、法律で保健所に動物取扱業者番号の登録が義務化されています。\n犬を探す人が、ブリーダーへ要望を出せば、動物取扱業者番号の詳細を見せてくれます。そこには、犬舎の名称、責任者名、登録日や有効期限の記載があります。\n良いブリーダーの見つけ方１つの例として、「登録日」に注視すると、いつからブリーダー業を営んでいるか分かります。\n何度か更新し「有効期限の期日」が延長されても（有効期限は5年）、「登録日」は最初に登録した日が記載されます。\n法律の施行は2006年6月からなので、それ以前の日付はありませんが、長年ブリーディングしている方なのか、登録して間もない新規参入したブリーダーなのか分かります。\n長く営業していれば必ず良いわけではありませんが、参考の１つになります。\nブリーダー探し、愛知から長野まで\nうさうさ さん\nもし質問主さんがブリーダー探しに労を惜しまないのであれば、近県まで出向くのも仕方ないと思います。\n希望する犬種の赤ちゃんが、すぐ近くで繁殖しタイミングよく産まれるなんて事は珍しいですからね〜＾＾；\nうちは愛知県から長野県まで片道3時間以上掛けて、ブリーダーの元へ子犬を見に行きました。\nそれともう一つ、獣医さん、トリマーさんを介して紹介して頂くのもイイかと思います。\nだいたいの上限価格を告げれば、多少時間が掛かるもののちゃんと対応して下さいますよ＾＾\nただ、獣医さんから仲介して頂いた場合、親犬を見る事は出来ない事が多い様です。でも獣医さんがしっかり選んで下さるなら安心です＾＾ 犬を飼う初期費用、1年間にかかる料金と必要リスト\nブリーダーの法律、対面の義務化\nダックス さん\nブリーダーで子犬の購入は、法律により一昔前と違う点があります。\nかつては、遠く離れたブリーダーと連絡をとり、代金を払えば、空輸や陸路で子犬を自宅まで届ける事業者もありました。\n2019年以降は、法律（改正動物愛護管理法）で禁止です。 *1\n犬の購入希望者は、ブリーダーが開業した事業所まで行き、犬の現物確認と対面説明を受ける義務があります。また、生後56日以下の子犬について譲渡禁止です。\n対面するため、時間とお金が必要になります。\nそれでもブリーダーから、直接、犬を購入する方は少なくありません。\nブリーダーの元へ車で数時間かける方もいれば、新幹線で子犬を迎えに行く方もいます。\nまた、先に現物確認と対面説明を受けにブリーダーを訪問し、後日、飛行機で輸送する方もいます。 犬と飛行機で旅行、料金やケージ規定サイズ\n*1 ・第一種動物取扱業者の規制\n- 販売時の対面説明:動物を購入しようとする者に対して、事業所において、その動物の現状を直接見せる（現物確認）と共に、その動物の特徴や適切な飼養方法等について対面で文書（電磁的記録を含む）を用いて説明（対面説明）することが必要となります。\n- ５６日齢以下の販売制限\n引用元: 環境省 - 動物愛護管理法\n販売数や口コミは？\n通りすがり さん\nネットで犬のブリーダーを紹介するサイトには、今までの販売数（契約数、譲渡数）、口コミや評価があります。\n数値データやユーザーの投稿コメントで、ブリーダーを探す方も少なくないです\nただ、販売数が多ければ良いかと言うと、それだけ多くの犬種や頭数を仲介・販売する大手ペット事業者となります。\n一方で、母犬に配慮したブリードする個人経営の犬舎さんは、年に数頭しか譲渡しないので、数値データは少なく、口コミ数も下位になります。\nどちらが良いではなく、数値データやコメント数のみで判断しないように注意してください。\nコメント内容は、本当に信用できるのか、苦情や批判の対応はどうか？\nコメント数が多いのに、高評価のコメントばかり、悪いコメントが少ないのは、何かあるのでは？と少し疑問を持つ。\n販売数や口コミは、とても有意義な情報です。が、全ての情報を鵜呑みにせず、参考程度が良いです。\nブリーダー探し、新聞の譲ります\nひ太 さん探している犬種はビーグルかダックスでしたね。ブリーダーさん探しでは意外な方法がありますよ。\n特に島根限定でお探しなら、新聞の譲りますコーナーをまめにチェックするのも手ですよ。\n中国地方では有名な某新聞の譲りますコーナーで、ペットのコーナーを探すとダックス、ビーグルだとか掲載されているのを見かけます。\n但し、ペット情報は土日の新聞にまとめて載るみたいで、\n平日のこともありますが、毎日載っていないので申し訳ないのだけれども・・・・それなりに県内のブリーダーさんの情報が載ると思います。\n犬種限定だと、1ヶ月くらいはチェックが必要ですが、人気犬種は良く載ってますよ。",
  "language": "ja",
  "image": "https:\/\/www.dogoo.com\/toukou\/dogqa\/pre\/pre_images\/whbrd.jpg?v1",
  "pagetype": "website",
  "filedate": "2024-06-12",
  "source": "https:\/\/www.dogoo.com\/toukou\/dogqa\/pre\/whbrd.htm",
  "source-hostname": "犬サイトdogoo.com",
  "excerpt": "犬ブリーダーの探し方、子犬の見つけ方を教えてください。今、新たに犬を飼おうと思っています。犬の入手方法を探すと「子犬の買い方は、ペットショップよりブリーダーから直に買うのが良い」との意見を多く目にします。直接ブリーダーから犬を買いたいのですが、良い犬の繁殖家ブリーダーは見つかりません。",
  "categories": "",
  "tags": "",
  "rejected": false,
  "rejected_reason": "",
  "commoncrawl_id": 0,
  "url": "https:\/\/dogoo.com\/toukou\/dogqa\/pre\/whbrd.htm"
}
```