FROM python:3

RUN pip3 install requests flask fuzzywuzzy python-levenshtein python-dateutil

COPY static /static/
COPY html /html
COPY metrics /metrics
COPY ngsi_ld /ngsi_ld
COPY other /other

ENV PORT 8081

ENV PORT 5000
EXPOSE $PORT


ADD config.ini configuration.py datasource_manager.py qoi_system.py semanticenrichment.py main.py /

CMD python3 main.py

