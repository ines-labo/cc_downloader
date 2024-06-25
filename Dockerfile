FROM python:3.10
LABEL authors="sakusakumura"

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv venv
ENV PATH="/home/venv/bin:$PATH"

COPY requirements.txt /root/

# requirements.txtをインストール
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN pip install -r /root/requirements.txt

COPY start.sh /root/
RUN chmod +x /root/start.sh
ENTRYPOINT ["/root/start.sh"]