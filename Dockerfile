FROM python:3.10-slim

WORKDIR /app

COPY ./api/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./api /app

CMD ["fastapi", "run", "/app/main.py", "--port", "80"]
