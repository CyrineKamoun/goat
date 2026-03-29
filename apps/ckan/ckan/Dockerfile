FROM ckan/ckan-base:2.10

USER root
RUN apk add --no-cache \
    geos-dev \
    proj-dev \
    gdal-dev \
    gcc \
    musl-dev \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    git

RUN pip install --no-cache-dir \
    pycsw \
    OWSLib \
    Shapely \
    pyproj \
    GeoAlchemy2 \
    requests \
    rdflib \
    ckantoolkit \
    geojson \
    pika

RUN pip install --no-cache-dir -e "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial" && \
    pip install --no-cache-dir -e "git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest" && \
    pip install --no-cache-dir -e "git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat"

# Make CSW harvest size configurable via source config key `max_datasets`.
RUN python3 - <<'PY'
from pathlib import Path

file_path = Path('/srv/app/src/ckanext-spatial/ckanext/spatial/harvesters/csw.py')
text = file_path.read_text(encoding='utf-8')
old = "for identifier in self.csw.getidentifiers(page=10, outputschema=self.output_schema(), cql=cql):"
new = "for identifier in self.csw.getidentifiers(page=10, outputschema=self.output_schema(), cql=cql, limit=self.source_config.get('max_datasets')):"

if old not in text:
    raise SystemExit('Expected CSW gather line not found; patch aborted.')

file_path.write_text(text.replace(old, new, 1), encoding='utf-8')
print('Patched CSW harvester with max_datasets support')
PY

COPY production.ini /etc/ckan/production.ini
COPY harvest_sources.json /etc/ckan/harvest_sources.json
COPY bootstrap_harvest_sources.py /usr/local/bin/bootstrap_harvest_sources.py
COPY start.sh /start.sh

RUN chmod +x /start.sh /usr/local/bin/bootstrap_harvest_sources.py

EXPOSE 5000
CMD ["/start.sh"]
