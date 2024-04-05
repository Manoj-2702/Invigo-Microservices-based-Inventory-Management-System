FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY stock_management.py .

CMD ["python", "stock_management.py"]