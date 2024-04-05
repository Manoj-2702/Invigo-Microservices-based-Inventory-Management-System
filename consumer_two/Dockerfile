FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY item_creation.py .

CMD ["python", "item_creation.py"]