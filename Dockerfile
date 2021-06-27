FROM akariv/dgp-app:latest

USER etl

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY configuration.json dags/
COPY logo.png ui/dist/ui/en/assets/logo.png
COPY favicons/* ui/dist/ui/

COPY logo.png site/static/assets/img/logo.png

COPY events dags/events
COPY operators dags/operators/
COPY srm_tools srm_tools
