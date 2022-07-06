FROM python:3.8-slim

ENV DAGSTER_HOME=/opt/dagster/app

WORKDIR $DAGSTER_HOME



RUN pip install nltk
RUN python -m nltk.downloader punkt

COPY dagster.yaml  $DAGSTER_HOME


RUN pip install --upgrade pip && pip install -r \
dagster_graphql \
gspread \
dagster \
dagit --no-cache-dir
COPY . .