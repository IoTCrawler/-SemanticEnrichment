FROM python:3

RUN pip3 install requests flask pymongo fuzzywuzzy python-levenshtein python-dateutil

COPY static /static/
COPY html /html
COPY metrics /metrics
COPY ngsi_ld /ngsi_ld
COPY other /other

ADD config.ini configuration.py datasource_manager.py qoi_system.py semanticenrichment.py main.py /

CMD python3 main.py

