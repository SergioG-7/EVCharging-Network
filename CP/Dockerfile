FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY EV_CP_M.py .
COPY EV_CP_E.py .
COPY EV_W.py .
ENTRYPOINT ["python","-u"]