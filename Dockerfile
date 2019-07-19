FROM python:3

RUN pip3 install requests flask

COPY static /static/
COPY html /html
COPY jsonfiles /jsonfiles
COPY metrics /metrics
COPY ngsi_ld /ngsi_ld
COPY other /other

ADD datasource_manager.py qoi_system.py semanticenrichment.py main.py /

CMD python3 main.py

