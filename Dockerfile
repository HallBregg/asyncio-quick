FROM python:3.10

WORKDIR /opt/app

COPY graceful_shutdown.py .

CMD ["python", "graceful_shutdown.py"]
