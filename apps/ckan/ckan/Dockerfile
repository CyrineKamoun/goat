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

# Patch ckanext-spatial ISO parser to handle gmx:Anchor elements.
# German CSW portals use <gmx:Anchor xlink:href="…">text</gmx:Anchor>
# where the XPaths only look for gco:CharacterString.  Without this,
# fields like protocol/otherConstraints/title come back empty and
# every record goes to "error" harvest state.
RUN python3 - <<'PY'
import re
from pathlib import Path

meta = Path('/srv/app/src/ckanext-spatial/ckanext/spatial/harvested_metadata.py')
text = meta.read_text(encoding='utf-8')

# gmx namespace is already registered in this version.
# Extend XPaths: ".../gco:CharacterString/text()" also try ".../gmx:Anchor/text()"
# We duplicate each search_path line that contains gco:CharacterString.
lines = text.split('\n')
new_lines = []
for line in lines:
    new_lines.append(line)
    # Only duplicate lines inside a list (ending with comma) — NOT keyword args
    # like search_paths="..." which would cause a SyntaxError if repeated.
    stripped = line.strip()
    if 'gco:CharacterString/text()' not in line:
        continue
    # Skip lines that are keyword arguments (search_paths=...)
    if 'search_paths=' in stripped:
        # Convert singular search_paths="xpath" to search_paths=["xpath", "anchor_xpath"]
        # by wrapping in a list with both variants.
        anchor_line = line.replace('gco:CharacterString', 'gmx:Anchor')
        # Extract the xpath string value
        import re as _re
        m = _re.search(r'search_paths\s*=\s*(["\'])(.+?)\1', line)
        if m:
            q = m.group(1)
            orig_xpath = m.group(2)
            anchor_xpath = orig_xpath.replace('gco:CharacterString', 'gmx:Anchor')
            old_kw = f'search_paths={q}{orig_xpath}{q}'
            new_kw = f'search_paths=[{q}{orig_xpath}{q}, {q}{anchor_xpath}{q}]'
            new_lines[-1] = line.replace(old_kw, new_kw)
    elif stripped.startswith('"') or stripped.startswith("'"):
        # Line is inside a list — safe to duplicate
        anchor_line = line.replace('gco:CharacterString', 'gmx:Anchor')
        new_lines.append(anchor_line)

text = '\n'.join(new_lines)
meta.write_text(text, encoding='utf-8')

count = sum(1 for l in new_lines if 'gmx:Anchor/text()' in l)
print(f'Added {count} gmx:Anchor XPath alternatives to harvested_metadata.py')
PY

COPY production.ini /etc/ckan/production.ini
COPY harvest_sources.json /etc/ckan/harvest_sources.json
COPY bootstrap_harvest_sources.py /usr/local/bin/bootstrap_harvest_sources.py
COPY start.sh /start.sh

RUN chmod +x /start.sh /usr/local/bin/bootstrap_harvest_sources.py

EXPOSE 5000
CMD ["/start.sh"]
