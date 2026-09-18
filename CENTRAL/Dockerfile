FROM python:3.8-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY EV_Central.py .
COPY database.py .
COPY chargingpoint.py .
COPY API_Central.py .
COPY Front.py .
ENTRYPOINT ["python"]