FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY recargas_cp.txt .
COPY certServ.pem .
COPY EV_Driver.py .
COPY EV_Registry.py .
ENTRYPOINT ["python","-u"]