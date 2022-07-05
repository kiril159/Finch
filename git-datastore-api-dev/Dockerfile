FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -U -r requirements.txt --no-cache-dir
COPY . .
