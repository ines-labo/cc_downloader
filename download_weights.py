import os
import requests
from tqdm import tqdm

# ft_weightsフォルダを作成
os.makedirs('./ft_weights', exist_ok=True)

# ダウンロードするファイルのURL
url = 'https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin'

# ファイルをダウンロード
response = requests.get(url, stream=True)
total_size = int(response.headers.get('content-length', 0))

with open('./ft_weights/lid.176.bin', 'wb') as file, tqdm(
    desc='Downloading',
    total=total_size,
    unit='iB',
    unit_scale=True,
    unit_divisor=1024,
) as progress_bar:
    for data in response.iter_content(chunk_size=1024):
        size = file.write(data)
        progress_bar.update(size)