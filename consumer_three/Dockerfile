FROM python:3.9-slim-buster

WORKDIR /app

COPY . .
RUN pip install -r requirements.txt

CMD ["python3", "stock_management.py"]