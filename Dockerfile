FROM python:3.10
LABEL authors="sakusakumura"

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv venv
ENV PATH="/home/venv/bin:$PATH"

COPY . /root/src

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN pip install -r /root/src/requirements.txt

RUN chmod +x /root/src/start.sh

# すべての .py ファイルと start.sh の改行コードを変換
RUN find /root/src -name "*.py" -type f -exec dos2unix {} +
RUN dos2unix /root/src/start.sh

ENTRYPOINT ["/bin/bash", "-c", "source /root/src/start.sh"]