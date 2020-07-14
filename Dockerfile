FROM python:3

RUN pip3 install requests flask fuzzywuzzy python-levenshtein python-dateutil

COPY static /static/
COPY html /html
COPY metrics /metrics
COPY ngsi_ld /ngsi_ld
COPY other /other

ENV PORT 8081
EXPOSE $PORT

ENV NGSI_ADDRESS 155.54.95.248:9090
ENV SE_HOST 0.0.0.0
ENV SE_PORT 8081
ENV SE_CALLBACK https://mobcom.ecs.hs-osnabrueck.de/semanticenrichment/callback

ADD config.ini configuration.py datasource_manager.py qoi_system.py semanticenrichment.py main.py /

CMD python3 main.py

