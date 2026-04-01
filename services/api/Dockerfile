FROM python:3.9-slim

WORKDIR /app/services/api

COPY app.py .

RUN pip install fastapi uvicorn pandas pyarrow

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]