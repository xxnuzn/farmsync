# FarmSync — 배포용 이미지
# 홈페이지(정적 파일)와 WebSocket 서버를 한 컨테이너로 함께 올린다.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FARMSYNC_DB=/data/farmsync.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server ./server
COPY web ./web

RUN mkdir -p /data

EXPOSE 8000

# 클라우드는 대부분 PORT 환경변수를 준다. 없으면 8000 을 쓴다.
CMD ["sh", "-c", "uvicorn server.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
