FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY order_processing.py .

CMD ["python", "order_processing.py"]