FROM xemuliam/dbt:1.7-snowflake

WORKDIR /usr/app

COPY dbt dbt/

COPY requirements.txt .
COPY src src/
COPY scripts scripts/

RUN chmod +x scripts/

RUN pip install -r requirements.txt --ignore-installed

ENV DBT_PROJECT_DIR=/usr/app/dbt
ENV DBT_PROFILES_DIR=/usr/app/dbt

WORKDIR /usr/app/dbt

RUN dbt deps
RUN pip install -U charset-normalizer

WORKDIR /usr/app
