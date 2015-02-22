FROM python:2.7.9

RUN adduser --disabled-password --home=/usr/src/app --gecos "" indexer

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install -r requirements.txt

COPY . /usr/src/app

RUN chown -R indexer:indexer /usr/src/app

USER indexer

CMD [ "python", "./start.py", "--useRabbit", "True" ]
