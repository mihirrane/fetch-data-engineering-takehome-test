#FROM python:3.8
FROM python:3.11-slim as build
ADD requirements.txt /
ADD config.ini /
RUN pip install -r requirements.txt
ADD sqs_to_postgress.py /
ADD main.py /
CMD ["sleep", "infinity"]