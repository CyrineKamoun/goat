from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from xml.etree import ElementTree as ET

import duckdb
import httpx
import psycopg
from psycopg.types.json import Jsonb

from .spatial_constants import (
    SPATIAL_EXTENSION_DETECT_ORDER,
    SPATIAL_EXTENSIONS,
    SPATIAL_URL_SUFFIXES,
)
LANGUAGE_CODE_MAP = {
    "ger": "de",
    "deu": "de",
    "de": "de",
    "eng": "en",
    "en": "en",
    "fra": "fr",
    "fr": "fr",
}

ISO3_TO_ISO2_COUNTRY_MAP = {
    "DEU": "DE",
    "FRA": "FR",
    "ESP": "ES",
    "ITA": "IT",
    "AUT": "AT",
    "CHE": "CH",
    "BEL": "BE",
    "NLD": "NL",
    "POL": "PL",
    "CZE": "CZ",
    "PRT": "PT",
    "LUX": "LU",
    "GBR": "GB",
    "UK": "GB",
    "USA": "US",
}

COUNTRY_NAME_TO_ISO2_MAP = {
    "germany": "DE",
    "deutschland": "DE",
    "france": "FR",
    "frankreich": "FR",
    "spain": "ES",
    "spanien": "ES",
    "italy": "IT",
    "italien": "IT",
    "austria": "AT",
    "osterreich": "AT",
    "österreich": "AT",
    "switzerland": "CH",
    "schweiz": "CH",
    "belgium": "BE",
    "belgien": "BE",
    "netherlands": "NL",
    "niederlande": "NL",
    "poland": "PL",
    "polen": "PL",
    "czech republic": "CZ",
    "tschechien": "CZ",
    "portugal": "PT",
    "luxembourg": "LU",
    "luxemburg": "LU",
    "united kingdom": "GB",
    "great britain": "GB",
    "vereinigtes konigreich": "GB",
    "vereinigtes königreich": "GB",
    "england": "GB",
    "usa": "US",
    "united states": "US",
    "vereinigte staaten": "US",
}

LICENSE_PATTERN_MAP: list[tuple[str, str]] = [
    ("cc-by-nc-nd", "CC_BY_NC_ND"),
    ("cc by nc nd", "CC_BY_NC_ND"),
    ("cc-by-nc-sa", "CC_BY_NC_SA"),
    ("cc by nc sa", "CC_BY_NC_SA"),
    ("cc-by-nc", "CC_BY_NC"),
    ("cc by nc", "CC_BY_NC"),
    ("cc-by-nd", "CC_BY_ND"),
    ("cc by nd", "CC_BY_ND"),
    ("cc-by-sa", "CC_BY_SA"),
    ("cc by sa", "CC_BY_SA"),
    ("cc-by", "CC_BY"),
    ("cc by", "CC_BY"),
    ("creativecommons.org/licenses/by/", "CC_BY"),
    ("creativecommons.org/licenses/by-sa/", "CC_BY_SA"),
    ("creativecommons.org/licenses/by-nd/", "CC_BY_ND"),
    ("creativecommons.org/licenses/by-nc/", "CC_BY_NC"),
    ("creativecommons.org/licenses/by-nc-sa/", "CC_BY_NC_SA"),
    ("creativecommons.org/licenses/by-nc-nd/", "CC_BY_NC_ND"),
    ("cc0", "CC_ZERO"),
    ("cc-zero", "CC_ZERO"),
    ("creativecommons.org/publicdomain/zero/", "CC_ZERO"),
    ("odc-by", "ODC_BY"),
    ("odc by", "ODC_BY"),
    ("odbl", "ODC_ODbL"),
    ("datenlizenz deutschland namensnennung 2.0", "DDN2"),
    ("datenlizenz deutschland zero 2.0", "DDZ2"),
]

logger = logging.getLogger(__name__)


def _extract_text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(element.itertext()).strip()
    return text or None


def _first_text(root: ET.Element, paths: list[str]) -> str | None:
    for path in paths:
        node = root.find(path)
        value = _extract_text(node)
        if value:
            return value
    return None


def _extract_language_code(root: ET.Element) -> str | None:
    node = root.find(".//{*}language//{*}LanguageCode")
    if node is not None:
        raw = (node.attrib.get("codeListValue") or "").strip().lower()
        if raw:
            return LANGUAGE_CODE_MAP.get(raw, raw[:2])

    text = _first_text(
        root,
        [
            ".//{*}language//{*}CharacterString",
        ],
    )
    if not text:
        return None
    raw = text.strip().lower()
    return LANGUAGE_CODE_MAP.get(raw, raw[:2] if len(raw) >= 2 else raw)


def _extract_year(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(19|20)\d{2}", value)
    if not match:
        return None
    return int(match.group(0))


def _normalize_country_code(value: str | None) -> str | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    lowered = raw.lower()
    name_match = COUNTRY_NAME_TO_ISO2_MAP.get(lowered)
    if name_match:
        return name_match

    # Handles authority URLs like .../country/DEU or .../country/DE
    authority_match = re.search(r"/country/([A-Za-z]{2,3})(?:$|[/?#])", raw)
    if authority_match:
        token = authority_match.group(1).upper()
        if len(token) == 2:
            return token
        return ISO3_TO_ISO2_COUNTRY_MAP.get(token)

    upper_raw = raw.upper()
    if re.fullmatch(r"[A-Z]{2}", upper_raw):
        return upper_raw
    if re.fullmatch(r"[A-Z]{3}", upper_raw):
        return ISO3_TO_ISO2_COUNTRY_MAP.get(upper_raw)

    prefixed_two = re.match(r"^([A-Za-z]{2})[-_:]", raw)
    if prefixed_two:
        return prefixed_two.group(1).upper()

    prefixed_three = re.match(r"^([A-Za-z]{3})[-_:]", raw)
    if prefixed_three:
        return ISO3_TO_ISO2_COUNTRY_MAP.get(prefixed_three.group(1).upper())

    for token in re.split(r"[^A-Za-z]+", raw):
        if not token:
            continue
        token_lower = token.lower()
        if token_lower in COUNTRY_NAME_TO_ISO2_MAP:
            return COUNTRY_NAME_TO_ISO2_MAP[token_lower]

        token_upper = token.upper()
        if len(token_upper) == 2:
            return token_upper
        if len(token_upper) == 3 and token_upper in ISO3_TO_ISO2_COUNTRY_MAP:
            return ISO3_TO_ISO2_COUNTRY_MAP[token_upper]

    return None


def _extract_geographical_code(root: ET.Element) -> str | None:
    geo_paths = [
        ".//{*}EX_GeographicDescription//{*}geographicIdentifier//{*}code//{*}CharacterString",
        ".//{*}EX_GeographicDescription//{*}geographicIdentifier//{*}code//{*}Anchor",
        ".//{*}EX_Extent//{*}description//{*}CharacterString",
    ]
    for path in geo_paths:
        for node in root.findall(path):
            code = _normalize_country_code(_extract_text(node))
            if code:
                return code

    return None


def _normalize_license_token(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    for pattern, target in LICENSE_PATTERN_MAP:
        if pattern in normalized:
            return target
    return None


def _extract_keywords(root: ET.Element) -> list[str]:
    keywords: list[str] = []
    seen: set[str] = set()

    for node in root.findall(".//{*}descriptiveKeywords//{*}keyword"):
        value = _first_text(
            node,
            [
                "{*}CharacterString",
                "{*}Anchor",
            ],
        )
        if value and value not in seen:
            seen.add(value)
            keywords.append(value)

    return keywords


def _extract_contacts(root: ET.Element) -> list[dict[str, str | None]]:
    contacts: list[dict[str, str | None]] = []
    for contact in root.findall(".//{*}CI_ResponsibleParty"):
        role_node = contact.find(".//{*}role//{*}CI_RoleCode")
        role = (
            role_node.attrib.get("codeListValue")
            if role_node is not None
            else None
        )
        contacts.append(
            {
                "role": role,
                "organization": _first_text(contact, [".//{*}organisationName//{*}CharacterString"]),
                "individual": _first_text(contact, [".//{*}individualName//{*}CharacterString"]),
                "email": _first_text(contact, [".//{*}electronicMailAddress//{*}CharacterString"]),
            }
        )
    return contacts


def _pick_primary_contact(contacts: list[dict[str, str | None]]) -> dict[str, str | None]:
    if not contacts:
        return {"organization": None, "email": None}

    role_priority = {
        "distributor": 0,
        "resourceProvider": 1,
        "owner": 2,
        "pointOfContact": 3,
        None: 99,
    }
    sorted_contacts = sorted(
        contacts,
        key=lambda item: role_priority.get(item.get("role"), 50),
    )
    top = sorted_contacts[0]
    return {
        "organization": top.get("organization"),
        "email": top.get("email"),
    }


def _extract_license_data(root: ET.Element) -> dict[str, Any]:
    texts: list[str] = []
    anchors: list[dict[str, str | None]] = []
    parsed_json: list[dict[str, Any]] = []

    for legal in root.findall(".//{*}MD_LegalConstraints"):
        has_use_constraints = legal.find(".//{*}useConstraints") is not None
        if not has_use_constraints:
            continue

        for other in legal.findall(".//{*}otherConstraints"):
            for string_node in other.findall(".//{*}CharacterString"):
                value = _extract_text(string_node)
                if not value:
                    continue
                texts.append(value)
                if value.startswith("{") and value.endswith("}"):
                    try:
                        raw_json = json.loads(value)
                        if isinstance(raw_json, dict):
                            parsed_json.append(raw_json)
                    except json.JSONDecodeError:
                        pass

            for anchor_node in other.findall(".//{*}Anchor"):
                label = _extract_text(anchor_node)
                anchors.append(
                    {
                        "label": label,
                        "href": anchor_node.attrib.get("{http://www.w3.org/1999/xlink}href"),
                    }
                )

    mapped_license: str | None = None
    attribution: str | None = None

    for item in parsed_json:
        mapped_license = mapped_license or _normalize_license_token(
            str(item.get("id") or item.get("name") or "")
        )
        if not attribution:
            quelle = item.get("quelle")
            if isinstance(quelle, str) and quelle.strip():
                attribution = quelle.strip()

    if not mapped_license:
        for value in texts:
            mapped_license = _normalize_license_token(value)
            if mapped_license:
                break

    if not mapped_license:
        for anchor in anchors:
            candidate = f"{anchor.get('label') or ''} {anchor.get('href') or ''}"
            mapped_license = _normalize_license_token(candidate)
            if mapped_license:
                break

    return {
        "license": mapped_license,
        "attribution": attribution,
        "license_texts": texts,
        "license_anchors": anchors,
        "license_json": parsed_json,
    }


def _extract_xml_metadata(xml_content: str | None) -> dict[str, Any]:
    output: dict[str, Any] = {
        "xml_found": bool(xml_content and xml_content.strip()),
        "name": None,
        "description": None,
        "language_code": None,
        "geographical_code": None,
        "distributor_name": None,
        "distributor_email": None,
        "distribution_url": None,
        "thumbnail_url": None,
        "license": None,
        "attribution": None,
        "data_reference_year": None,
    }

    if not output["xml_found"]:
        return output

    try:
        root = ET.fromstring(str(xml_content))
    except ET.ParseError as exc:
        output["xml_parse_error"] = str(exc)
        output["xml_found"] = False
        return output

    output["name"] = _first_text(
        root,
        [
            ".//{*}identificationInfo//{*}citation//{*}title//{*}CharacterString",
            ".//{*}identificationInfo//{*}citation//{*}title//{*}Anchor",
        ],
    )
    output["description"] = _first_text(
        root,
        [
            ".//{*}identificationInfo//{*}abstract//{*}CharacterString",
            ".//{*}identificationInfo//{*}abstract//{*}Anchor",
        ],
    )
    output["language_code"] = _extract_language_code(root)
    output["geographical_code"] = _extract_geographical_code(root)

    contacts = _extract_contacts(root)
    primary_contact = _pick_primary_contact(contacts)
    output["distributor_name"] = primary_contact.get("organization")
    output["distributor_email"] = primary_contact.get("email")

    output["distribution_url"] = _first_text(
        root,
        [
            ".//{*}distributionInfo//{*}onLine//{*}URL",
            ".//{*}distributionInfo//{*}onLine//{*}linkage//{*}CharacterString",
        ],
    )
    output["thumbnail_url"] = _first_text(
        root,
        [
            ".//{*}graphicOverview//{*}fileName//{*}CharacterString",
            ".//{*}graphicOverview//{*}fileName//{*}URL",
        ],
    )

    license_data = _extract_license_data(root)
    output["license"] = license_data.get("license")
    output["attribution"] = license_data.get("attribution")

    date_value = _first_text(
        root,
        [
            ".//{*}identificationInfo//{*}citation//{*}CI_Date//{*}Date",
            ".//{*}identificationInfo//{*}citation//{*}CI_Date//{*}DateTime",
            ".//{*}date//{*}Date",
            ".//{*}date//{*}DateTime",
        ],
    )
    output["data_reference_year"] = _extract_year(date_value)
    return output


def _extract_bbox(root: ET.Element) -> dict[str, float] | None:
    boxes = root.findall(".//{*}EX_GeographicBoundingBox")
    for box in boxes:
        west = _first_text(box, [".//{*}westBoundLongitude//{*}Decimal", ".//{*}westBoundLongitude//{*}Real"])
        south = _first_text(box, [".//{*}southBoundLatitude//{*}Decimal", ".//{*}southBoundLatitude//{*}Real"])
        east = _first_text(box, [".//{*}eastBoundLongitude//{*}Decimal", ".//{*}eastBoundLongitude//{*}Real"])
        north = _first_text(box, [".//{*}northBoundLatitude//{*}Decimal", ".//{*}northBoundLatitude//{*}Real"])
        if not all([west, south, east, north]):
            continue
        try:
            return {
                "west": float(west),
                "south": float(south),
                "east": float(east),
                "north": float(north),
            }
        except ValueError:
            continue
    return None


def _build_csw_record(row: dict[str, Any]) -> dict[str, Any]:
    resource_id = str(row.get("resource_id") or "")
    package_id = str(row.get("package_id") or "")
    xml_content = str(row.get("xml_content") or "")
    metadata_modified = row.get("metadata_modified")

    fallback_title = (
        str(row.get("package_title") or "").strip()
        or str(row.get("package_name") or "").strip()
        or resource_id
    )

    base_record: dict[str, Any] = {
        "identifier": resource_id,
        "title": fallback_title,
        "abstract": str(row.get("package_notes") or "").strip() or None,
        "keywords": [],
        "language": None,
        "geographical_code": None,
        "contacts": [],
        "license": None,
        "attribution": None,
        "bbox": None,
        "updated_at": metadata_modified.isoformat() if hasattr(metadata_modified, "isoformat") else str(metadata_modified or ""),
        "source": {
            "package_id": package_id,
            "resource_id": resource_id,
            "harvest_guid": str(row.get("harvest_guid") or "") or None,
        },
    }

    if not xml_content:
        return base_record

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return base_record

    title = _first_text(
        root,
        [
            ".//{*}identificationInfo//{*}citation//{*}title//{*}CharacterString",
            ".//{*}identificationInfo//{*}citation//{*}title//{*}Anchor",
            ".//{*}title//{*}CharacterString",
        ],
    )
    abstract = _first_text(
        root,
        [
            ".//{*}identificationInfo//{*}abstract//{*}CharacterString",
            ".//{*}identificationInfo//{*}abstract//{*}Anchor",
        ],
    )
    keywords = _extract_keywords(root)
    contacts = _extract_contacts(root)
    license_data = _extract_license_data(root)

    base_record["title"] = title or fallback_title
    base_record["abstract"] = abstract or base_record["abstract"]
    base_record["keywords"] = keywords
    base_record["language"] = _extract_language_code(root)
    base_record["geographical_code"] = _extract_geographical_code(root)
    base_record["contacts"] = contacts
    base_record["license"] = license_data.get("license")
    base_record["attribution"] = license_data.get("attribution")
    base_record["bbox"] = _extract_bbox(root)
    return base_record

def _resolve_required_secret(value: str | None, env_keys: tuple[str, ...], *, label: str) -> str:
    if value:
        return value

    for env_key in env_keys:
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

    keys = ", ".join(env_keys)
    raise ValueError(f"Missing required credential for {label}. Provide CLI argument or set one of: {keys}")


def _parse_datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        candidate = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None
    return None


def _safe_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_xml_content_from_package(package: dict[str, Any]) -> str:
    # Prefer explicit XML-like extras/fields when available.
    candidates: list[str | None] = []
    extras = package.get("extras")
    if isinstance(extras, list):
        for extra in extras:
            if not isinstance(extra, dict):
                continue
            key = str(extra.get("key") or "").strip().lower()
            if key in {"xml", "xml_content", "metadata_xml", "csw_xml", "iso19139"}:
                candidates.append(_safe_string(extra.get("value")))

    for key in ("xml", "xml_content", "metadata_xml", "csw_xml"):
        candidates.append(_safe_string(package.get(key)))

    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def _build_ckan_headers(api_key: str | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = api_key
    return headers


def _repo_root() -> Path:
    # .../apps/datacatalog/src/datacatalog/pipeline/duckdb_sync.py -> repo root
    return Path(__file__).resolve().parents[4]


def _enable_goatlib_imports() -> None:
    """Add goatlib source path to sys.path when running from this app package."""
    goatlib_src = _repo_root() / "packages/python/goatlib/src"
    goatlib_src_str = str(goatlib_src)
    if goatlib_src.exists() and goatlib_src_str not in sys.path:
        sys.path.insert(0, goatlib_src_str)


def _is_spatial_candidate(resource: dict[str, Any]) -> bool:
    # Support both raw CKAN resource payload keys and DB row aliases.
    fmt = str(resource.get("resource_format") or resource.get("format") or "").strip().lower()
    mimetype = str(resource.get("resource_mimetype") or resource.get("mimetype") or "").strip().lower()
    url = str(resource.get("resource_url") or resource.get("url") or "").strip().lower()

    if fmt in SPATIAL_EXTENSIONS:
        return True
    if "wfs" in fmt or "wfs" in mimetype:
        return True
    if "geo" in fmt or "spatial" in mimetype:
        return True
    if "service=wfs" in url or "request=getfeature" in url:
        return True
    return url.endswith(SPATIAL_URL_SUFFIXES)


def _detect_ext(resource_url: str, resource_format: str | None) -> str:
    url = resource_url.split("?")[0].lower()
    for ext in SPATIAL_EXTENSION_DETECT_ORDER:
        if url.endswith(f".{ext}"):
            return ext
    if resource_format:
        fmt = resource_format.strip().lower()
        if fmt in SPATIAL_EXTENSIONS:
            return fmt
    return "geojson"


def _is_wfs_resource(resource: dict[str, Any]) -> bool:
    fmt = str(resource.get("resource_format") or resource.get("format") or "").strip().lower()
    mimetype = str(resource.get("resource_mimetype") or resource.get("mimetype") or "").strip().lower()
    url = str(resource.get("resource_url") or resource.get("url") or "").strip().lower()
    return (
        fmt == "wfs"
        or "wfs" in fmt
        or "wfs" in mimetype
        or "service=wfs" in url
        or "/wfs" in url
    )


def _extract_wfs_layer_name(url: str) -> str | None:
    """Best-effort extraction of layer/typeName from common WFS URL forms."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    for key in ("typeNames", "typenames", "typeName", "typename"):
        value = params.get(key)
        if value and value[0].strip():
            return value[0].strip()

    parts = [part for part in parsed.path.split("/") if part]
    for idx, part in enumerate(parts):
        if part.lower() == "wfs" and idx + 1 < len(parts):
            layer = parts[idx + 1].strip()
            if layer:
                return layer

    return None


def _download_wfs_via_goatlib(url: str) -> tuple[str, str]:
    """Use GOAT's goatlib WFS pipeline to fetch WFS and return local Parquet path + temp dir."""
    try:
        _enable_goatlib_imports()
        from goatlib.io.remote_source.wfs import from_wfs  # type: ignore

        temp_dir = Path(tempfile.mkdtemp(prefix="ckan_wfs_"))
        layer_name = _extract_wfs_layer_name(url)
        result = from_wfs(
            url=url,
            out_dir=temp_dir,
            layer=layer_name,
            target_crs="EPSG:4326",
        )

        if not result or result == (None, None):
            raise RuntimeError(f"No data returned from WFS source: {url}")

        if isinstance(result, list):
            parquet_path, _metadata = result[0]
        else:
            parquet_path, _metadata = result

        return str(parquet_path), str(temp_dir)
    except Exception:
        # Fallback inspired by GOAT WFS flow: normalize URL and request features as GeoJSON.
        return _download_wfs_as_geojson(url)


def _clean_wfs_url(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params_to_remove = {"service", "request", "version", "SERVICE", "REQUEST", "VERSION"}
    cleaned_params = {k: v for k, v in params.items() if k not in params_to_remove}
    new_query = urlencode(cleaned_params, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _download_wfs_as_geojson(url: str) -> tuple[str, str]:
    temp_dir = Path(tempfile.mkdtemp(prefix="ckan_wfs_geojson_"))
    out_path = temp_dir / "wfs.geojson"

    layer_name = _extract_wfs_layer_name(url)

    base_url = _clean_wfs_url(url)
    capability_names = _fetch_wfs_typenames(url)

    name_candidates: list[str] = []
    if layer_name:
        name_candidates.append(layer_name)
        for name in capability_names:
            if name.endswith(f":{layer_name}"):
                name_candidates.append(name)
    name_candidates.extend(capability_names)

    seen: set[str] = set()
    unique_name_candidates: list[str] = []
    for name in name_candidates:
        if name and name not in seen:
            seen.add(name)
            unique_name_candidates.append(name)

    attempts: list[dict[str, str]] = []
    for type_name in unique_name_candidates:
        attempts.extend(
            [
                {
                    "service": "WFS",
                    "version": "2.0.0",
                    "request": "GetFeature",
                    "typeNames": type_name,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:4326",
                },
                {
                    "service": "WFS",
                    "version": "1.1.0",
                    "request": "GetFeature",
                    "typeName": type_name,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:4326",
                },
            ]
        )

    if not attempts:
        attempts.append(
            {
                "service": "WFS",
                "request": "GetFeature",
                "outputFormat": "application/json",
            }
        )

    last_error = ""
    with httpx.Client(timeout=180.0, follow_redirects=True) as client:
        for params in attempts:
            try:
                response = client.get(base_url, params=params)
                response.raise_for_status()
                payload = response.text
                if "FeatureCollection" not in payload and "features" not in payload:
                    raise RuntimeError("WFS response is not GeoJSON")
                out_path.write_text(payload, encoding="utf-8")
                return str(out_path), str(temp_dir)
            except Exception as exc:
                last_error = str(exc)

    raise RuntimeError(f"WFS GetFeature GeoJSON download failed: {last_error}")


def _fetch_wfs_typenames(url: str) -> list[str]:
    """Extract feature type names from WFS GetCapabilities response."""
    base_url = _clean_wfs_url(url)
    params = {"service": "WFS", "request": "GetCapabilities"}

    try:
        with httpx.Client(timeout=120.0, follow_redirects=True) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()

        root = ET.fromstring(response.content)
        names: list[str] = []
        tags = [
            ".//{http://www.opengis.net/wfs/2.0}Name",
            ".//{http://www.opengis.net/wfs}Name",
        ]
        for tag in tags:
            for element in root.findall(tag):
                if element.text and element.text.strip():
                    names.append(element.text.strip())
        return names
    except Exception:
        return []


def _signature(row: dict[str, Any]) -> str:
    # Data signature controls whether a new DuckLake table version is created.
    # Metadata-only changes (e.g. XML/CSW updates) should not duplicate data tables.
    resource_hash = str(row.get("resource_hash") or "").strip()
    if resource_hash:
        raw = "|".join(
            [
                str(row.get("package_id") or ""),
                str(row.get("resource_id") or ""),
                resource_hash,
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    raw = "|".join(
        [
            str(row.get("package_id") or ""),
            str(row.get("resource_id") or ""),
            str(row.get("resource_url") or ""),
            str(row.get("resource_format") or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _dataset_hash(package_id: str, resource_id: str) -> str:
    raw = f"{package_id}:{resource_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]


def _validate_schema_name(schema: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _ensure_tables(conn: psycopg.Connection, *, schema: str) -> None:
    schema = _validate_schema_name(schema)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.processor_dataset_version (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                package_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                signature TEXT NOT NULL,
                version_num INTEGER NOT NULL,
                duckdb_path TEXT NOT NULL,
                duckdb_table TEXT NOT NULL,
                row_count BIGINT,
                status TEXT NOT NULL,
                error TEXT,
                processed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_processor_dataset_signature
            ON {schema}.processor_dataset_version(resource_id, signature)
            """
        )
        # Materialized catalog layer metadata for GeoAPI resolution.
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.layer (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                resource_id TEXT NOT NULL,
                base_resource_id TEXT,
                version_num INTEGER,
                is_latest BOOLEAN,
                superseded_at TIMESTAMPTZ,
                name TEXT NOT NULL,
                feature_layer_geometry_type TEXT,
                extent geometry(MultiPolygon, 4326),
                csw_record_jsonb JSONB,
                csw_raw_xml TEXT,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL
            )
            """
        )
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS base_resource_id TEXT")
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS version_num INTEGER")
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS is_latest BOOLEAN")
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ")
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS csw_record_jsonb JSONB")
        cur.execute(f"ALTER TABLE {schema}.layer ADD COLUMN IF NOT EXISTS csw_raw_xml TEXT")
        cur.execute(
            f"""
            UPDATE {schema}.layer
            SET base_resource_id = COALESCE(base_resource_id, resource_id),
                version_num = COALESCE(version_num, 1),
                is_latest = COALESCE(is_latest, TRUE)
            """
        )
        cur.execute(f"DROP INDEX IF EXISTS {schema}.ux_layer_resource")
        cur.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_layer_resource_latest
            ON {schema}.layer(resource_id)
            WHERE is_latest
            """
        )
        cur.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_layer_resource_version
            ON {schema}.layer(resource_id, version_num)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS ix_layer_resource_id
            ON {schema}.layer(resource_id)
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS ix_layer_csw_identifier
            ON {schema}.layer((csw_record_jsonb->>'identifier'))
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS ix_layer_csw_title
            ON {schema}.layer((csw_record_jsonb->>'title'))
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS ix_layer_csw_record_gin
            ON {schema}.layer USING GIN (csw_record_jsonb)
            """
        )


def _upsert_catalog_layer(
    conn: psycopg.Connection,
    *,
    schema: str,
    resource_id: str,
    name: str,
    geometry_type: str | None,
    extent_wkt: str | None,
    csw_record_jsonb: dict[str, Any] | None,
    csw_raw_xml: str | None,
    schema_name: str,
    table_name: str,
    version_num: int | None = None,
    create_new_version: bool = False,
) -> str:
    """Insert or update catalog layer rows while preserving historical versions."""
    schema = _validate_schema_name(schema)
    with conn.cursor() as cur:
        if create_new_version:
            resolved_version_num = int(version_num) if version_num is not None else 1
            cur.execute(
                f"""
                UPDATE {schema}.layer
                SET is_latest = FALSE,
                    superseded_at = NOW()
                WHERE resource_id = %s
                  AND is_latest
                """,
                (resource_id,),
            )
            cur.execute(
                f"""
                INSERT INTO {schema}.layer (
                    resource_id,
                    base_resource_id,
                    version_num,
                    is_latest,
                    superseded_at,
                    name,
                    feature_layer_geometry_type,
                    extent,
                    csw_record_jsonb,
                    csw_raw_xml,
                    schema_name,
                    table_name
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    TRUE,
                    NULL,
                    %s,
                    %s,
                    CASE WHEN %s::text IS NOT NULL
                         THEN ST_Multi(ST_GeomFromText(%s, 4326))
                         ELSE NULL END,
                    %s,
                    %s,
                    %s,
                    %s
                )
                RETURNING id
                """,
                (
                    resource_id,
                    resource_id,
                    resolved_version_num,
                    name,
                    geometry_type,
                    extent_wkt,
                    extent_wkt,
                    Jsonb(csw_record_jsonb) if csw_record_jsonb else None,
                    csw_raw_xml,
                    schema_name,
                    table_name,
                ),
            )
            row = cur.fetchone()
            return str(row[0])

        cur.execute(
            f"""
            UPDATE {schema}.layer
            SET
                name = %s,
                feature_layer_geometry_type = %s,
                extent = CASE WHEN %s::text IS NOT NULL
                              THEN ST_Multi(ST_GeomFromText(%s, 4326))
                              ELSE NULL END,
                csw_record_jsonb = %s,
                csw_raw_xml = %s,
                schema_name = %s,
                table_name = %s
            WHERE resource_id = %s
              AND is_latest
            RETURNING id
            """,
            (
                name,
                geometry_type,
                extent_wkt,
                extent_wkt,
                Jsonb(csw_record_jsonb) if csw_record_jsonb else None,
                csw_raw_xml,
                schema_name,
                table_name,
                resource_id,
            ),
        )
        row = cur.fetchone()
        if row:
            return str(row[0])

        resolved_version_num = int(version_num) if version_num is not None else 1
        cur.execute(
            f"""
            INSERT INTO {schema}.layer (
                resource_id,
                base_resource_id,
                version_num,
                is_latest,
                superseded_at,
                name,
                feature_layer_geometry_type,
                extent,
                csw_record_jsonb,
                csw_raw_xml,
                schema_name,
                table_name
            ) VALUES (
                %s,
                %s,
                %s,
                TRUE,
                NULL,
                %s,
                %s,
                CASE WHEN %s::text IS NOT NULL
                     THEN ST_Multi(ST_GeomFromText(%s, 4326))
                     ELSE NULL END,
                %s,
                %s,
                %s,
                %s
            )
            RETURNING id
            """,
            (
                resource_id,
                resource_id,
                resolved_version_num,
                name,
                geometry_type,
                extent_wkt,
                extent_wkt,
                Jsonb(csw_record_jsonb) if csw_record_jsonb else None,
                csw_raw_xml,
                schema_name,
                table_name,
            ),
        )
        inserted = cur.fetchone()
        return str(inserted[0])


def _get_existing_catalog_layer(
    conn: psycopg.Connection,
    *,
    schema: str,
    resource_id: str,
) -> dict[str, Any] | None:
    """Get existing catalog layer pointer/geometry for metadata-only refreshes."""
    schema = _validate_schema_name(schema)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT
                id::text AS id,
                schema_name,
                table_name,
                feature_layer_geometry_type,
                CASE WHEN extent IS NOT NULL THEN ST_AsText(extent) END AS extent_wkt
            FROM {schema}.layer
            WHERE resource_id = %s
                            AND is_latest
                        ORDER BY version_num DESC NULLS LAST
            LIMIT 1
            """,
            (resource_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _load_candidates(
    *,
    package_search_url: str,
    api_key: str | None,
    page_size: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    headers = _build_ckan_headers(api_key)

    with httpx.Client(timeout=60.0, follow_redirects=True, headers=headers) as client:
        for page_index in range(max_pages):
            start = page_index * page_size
            params = {
                "rows": page_size,
                "start": start,
                "include_private": "true",
            }
            response = client.get(package_search_url, params=params)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict) or not payload.get("success"):
                raise RuntimeError("CKAN package_search did not return a success payload")

            result = payload.get("result")
            if not isinstance(result, dict):
                break

            packages = result.get("results")
            if not isinstance(packages, list) or not packages:
                break

            for package in packages:
                if not isinstance(package, dict):
                    continue
                if str(package.get("state") or "active").lower() != "active":
                    continue

                package_id = _safe_string(package.get("id")) or ""
                metadata_modified = _parse_datetime_or_none(package.get("metadata_modified"))
                package_name = _safe_string(package.get("name")) or ""
                package_title = _safe_string(package.get("title")) or ""
                package_notes = _safe_string(package.get("notes")) or ""
                xml_content = _extract_xml_content_from_package(package)

                resources = package.get("resources")
                if not isinstance(resources, list):
                    continue

                for resource in resources:
                    if not isinstance(resource, dict):
                        continue
                    if str(resource.get("state") or "active").lower() != "active":
                        continue

                    candidate = {
                        "package_id": package_id,
                        "metadata_modified": metadata_modified,
                        "package_name": package_name,
                        "package_title": package_title,
                        "package_notes": package_notes,
                        "resource_id": _safe_string(resource.get("id")) or "",
                        "resource_url": _safe_string(resource.get("url")) or "",
                        "resource_format": _safe_string(resource.get("format")) or "",
                        "resource_mimetype": _safe_string(resource.get("mimetype")) or "",
                        "resource_hash": _safe_string(resource.get("hash")) or "",
                        "xml_content": xml_content,
                        "harvest_guid": _safe_string(package.get("id")),
                    }
                    if _is_spatial_candidate(candidate):
                        candidates.append(candidate)

            if len(packages) < page_size:
                break

    return candidates


def _get_latest_signature(conn: psycopg.Connection, resource_id: str, *, schema: str) -> str | None:
    schema = _validate_schema_name(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT signature
            FROM {schema}.processor_dataset_version
            WHERE resource_id = %s AND status = 'success'
            ORDER BY version_num DESC
            LIMIT 1
            """,
            (resource_id,),
        )
        record = cur.fetchone()
    return str(record[0]) if record else None


def _next_version(conn: psycopg.Connection, resource_id: str, *, schema: str) -> int:
    schema = _validate_schema_name(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COALESCE(MAX(version_num), 0) + 1
            FROM {schema}.processor_dataset_version
            WHERE resource_id = %s
            """,
            (resource_id,),
        )
        value = cur.fetchone()
    return int(value[0]) if value else 1


def _delete_failed_signature(
    conn: psycopg.Connection,
    *,
    schema: str,
    resource_id: str,
    signature: str,
) -> None:
    schema = _validate_schema_name(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {schema}.processor_dataset_version
            WHERE resource_id = %s AND signature = %s AND status = 'failed'
            """,
            (resource_id, signature),
        )


def _download(url: str) -> str:
    suffix = Path(url.split("?")[0]).suffix or ".bin"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name

    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as file_handle:
                for chunk in response.iter_bytes(chunk_size=1024 * 128):
                    file_handle.write(chunk)

    return tmp_path


def _convert_to_goatlib_parquet(src_path: str, *, dest_dir: str) -> str:
    """Convert a downloaded file to GOAT-standard parquet via goatlib."""
    _enable_goatlib_imports()
    from goatlib.io.ingest import convert_any  # type: ignore

    outputs = convert_any(src_path=src_path, dest_dir=dest_dir, target_crs="EPSG:4326")
    if not outputs:
        raise RuntimeError(f"goatlib conversion produced no output for source: {src_path}")

    out_path, _metadata = outputs[0]
    return str(out_path)


def _load_into_duckdb(
    duckdb_path: str,
    file_path: str,
    table_schema: str,
    table_name: str,
    ext: str,
) -> int:
    table_schema = _validate_schema_name(table_schema)
    full_table_name = f"{table_schema}.{table_name}"

    # Keep this aligned with DuckDB supported values; explicit patch version strings
    # can break on newer runtimes.
    conn = duckdb.connect(
        duckdb_path,
        config={"storage_compatibility_version": "latest"},
    )
    try:
        # Required for geometry columns that include CRS metadata.
        conn.execute("PRAGMA storage_compatibility_version='latest';")
    except Exception:
        # Some DuckDB builds may not expose this pragma; continue best-effort.
        pass
    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
    except Exception:
        # Spatial extension can already be installed or unavailable depending on runtime.
        pass

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")

    if ext in {"parquet"}:
        conn.execute(
            f"CREATE OR REPLACE TABLE {full_table_name} AS "
            f"SELECT * FROM read_parquet('{file_path}')"
        )
    elif ext in {"csv"}:
        conn.execute(
            f"CREATE OR REPLACE TABLE {full_table_name} AS "
            f"SELECT * FROM read_csv_auto('{file_path}')"
        )
    elif ext in {"zip"}:
        conn.execute(
            f"CREATE OR REPLACE TABLE {full_table_name} AS "
            f"SELECT * FROM ST_Read('/vsizip/{file_path}')"
        )
    else:
        conn.execute(
            f"CREATE OR REPLACE TABLE {full_table_name} AS "
            f"SELECT * FROM ST_Read('{file_path}')"
        )

    row_count = int(conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0])
    conn.close()
    return row_count


DUCKLAKE_KEEPALIVE_PARAMS = {
    "keepalives": "1",
    "keepalives_idle": "30",
    "keepalives_interval": "5",
    "keepalives_count": "5",
}


def _load_into_ducklake(
    *,
    file_path: str,
    table_schema: str,
    table_name: str,
    ext: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    pg_user: str,
    pg_password: str,
    ducklake_data_dir: str,
    ducklake_catalog_schema: str,
) -> tuple[int, str | None, str | None]:
    """Load data into DuckLake and return row count plus geometry summary."""
    table_schema = _validate_schema_name(table_schema)
    full_table_name = f"lake.{table_schema}.{table_name}"

    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _map_geometry_type(raw_type: str | None) -> str | None:
        if not raw_type:
            return None
        normalized = raw_type.upper().replace("ST_", "")
        if "POINT" in normalized:
            return "point"
        if "LINE" in normalized:
            return "line"
        if "POLYGON" in normalized:
            return "polygon"
        return None

    # Ensure the schema subdirectory exists so DuckLake can write parquet files.
    os.makedirs(os.path.join(ducklake_data_dir, table_schema), exist_ok=True)

    conn = duckdb.connect()
    try:
        for ext_name in ("spatial", "httpfs", "postgres", "ducklake"):
            try:
                conn.execute(f"INSTALL {ext_name};")
            except Exception:
                pass
            conn.execute(f"LOAD {ext_name};")

        params = {
            "host": pg_host,
            "port": str(pg_port),
            "dbname": pg_db,
            "user": pg_user,
            "password": pg_password,
        }
        params.update(DUCKLAKE_KEEPALIVE_PARAMS)
        libpq_str = " ".join(f"{k}={v}" for k, v in params.items())

        conn.execute(
            f"ATTACH 'ducklake:postgres:{libpq_str}' AS lake ("
            f"DATA_PATH '{ducklake_data_dir}', "
            f"METADATA_SCHEMA '{ducklake_catalog_schema}', "
            f"OVERRIDE_DATA_PATH TRUE"
            f")"
        )

        conn.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{table_schema}")

        if ext in {"parquet"}:
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table_name} AS "
                f"SELECT * FROM read_parquet('{file_path}')"
            )
        elif ext in {"csv"}:
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table_name} AS "
                f"SELECT * FROM read_csv_auto('{file_path}')"
            )
        elif ext in {"zip"}:
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table_name} AS "
                f"SELECT * FROM ST_Read('/vsizip/{file_path}')"
            )
        else:
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table_name} AS "
                f"SELECT * FROM ST_Read('{file_path}')"
            )

        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()[0])

        geometry_type: str | None = None
        extent_wkt: str | None = None

        describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {full_table_name}").fetchall()
        geometry_columns = [
            str(row[0])
            for row in describe_rows
            if len(row) > 1 and "GEOMETRY" in str(row[1]).upper()
        ]
        if geometry_columns:
            geom_col = _quote_identifier(geometry_columns[0])
            geom_type_row = conn.execute(
                f"SELECT ST_GeometryType({geom_col}) FROM {full_table_name} WHERE {geom_col} IS NOT NULL LIMIT 1"
            ).fetchone()
            geometry_type = _map_geometry_type(
                str(geom_type_row[0]) if geom_type_row and geom_type_row[0] else None
            )

            bounds_row = conn.execute(
                (
                    f"SELECT "
                    f"ST_XMin(ST_Extent_Agg({geom_col})), "
                    f"ST_YMin(ST_Extent_Agg({geom_col})), "
                    f"ST_XMax(ST_Extent_Agg({geom_col})), "
                    f"ST_YMax(ST_Extent_Agg({geom_col})) "
                    f"FROM {full_table_name}"
                )
            ).fetchone()
            if bounds_row and all(value is not None for value in bounds_row):
                min_x, min_y, max_x, max_y = bounds_row
                extent_wkt = (
                    f"POLYGON(({min_x} {min_y}, {max_x} {min_y}, "
                    f"{max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"
                )

        return row_count, geometry_type, extent_wkt
    finally:
        conn.close()


def run_sync(
    *,
    ckan_package_search_url: str,
    ckan_api_key: str | None,
    ckan_api_page_size: int,
    ckan_api_max_pages: int,
    metadata_pg_host: str,
    metadata_pg_port: int,
    metadata_pg_db: str,
    metadata_pg_user: str,
    metadata_pg_password: str | None,
    metadata_pg_schema: str,
    duckdb_path: str,
    duckdb_schema: str,
    ducklake_data_dir: str,
    ducklake_catalog_schema: str = "ducklake",
) -> dict[str, Any]:
    resolved_metadata_pg_password = _resolve_required_secret(
        metadata_pg_password,
        ("META_PG_PASSWORD", "METADATA_PG_PASSWORD", "POSTGRES_PASSWORD"),
        label="metadata postgres password",
    )

    os.makedirs(Path(duckdb_path).parent, exist_ok=True)
    run_id = str(uuid.uuid4())
    logger.info(
        "sync started run_id=%s duckdb=%s/%s metadata=%s:%s/%s schema=%s",
        run_id,
        duckdb_path,
        duckdb_schema,
        metadata_pg_host,
        metadata_pg_port,
        metadata_pg_db,
        metadata_pg_schema,
    )

    processed = 0
    skipped = 0
    failed = 0

    schema = _validate_schema_name(metadata_pg_schema)

    candidates = _load_candidates(
        package_search_url=ckan_package_search_url,
        api_key=ckan_api_key,
        page_size=ckan_api_page_size,
        max_pages=ckan_api_max_pages,
    )
    logger.info("loaded %s candidate resources from ckan api", len(candidates))

    with psycopg.connect(
        host=metadata_pg_host,
        port=metadata_pg_port,
        dbname=metadata_pg_db,
        user=metadata_pg_user,
        password=resolved_metadata_pg_password,
        autocommit=False,
    ) as metadata_conn:
        _ensure_tables(metadata_conn, schema=schema)

        for index, row in enumerate(candidates, start=1):
            package_id = str(row["package_id"])
            resource_id = str(row["resource_id"])
            logger.info(
                "candidate %s/%s package_id=%s resource_id=%s format=%s",
                index,
                len(candidates),
                package_id,
                resource_id,
                row.get("resource_format"),
            )
            signature = _signature(row)
            last_signature = _get_latest_signature(metadata_conn, resource_id, schema=schema)

            if signature == last_signature:
                # Data unchanged: refresh catalog metadata in-place without creating
                # a new DuckLake data table version.
                existing_layer = _get_existing_catalog_layer(
                    metadata_conn,
                    schema=schema,
                    resource_id=resource_id,
                )
                if existing_layer:
                    csw_record = _build_csw_record(row)
                    layer_name = str(csw_record.get("title") or resource_id)

                    _upsert_catalog_layer(
                        metadata_conn,
                        schema=schema,
                        resource_id=resource_id,
                        name=layer_name,
                        geometry_type=(
                            str(existing_layer.get("feature_layer_geometry_type"))
                            if existing_layer.get("feature_layer_geometry_type") is not None
                            else None
                        ),
                        extent_wkt=(
                            str(existing_layer.get("extent_wkt"))
                            if existing_layer.get("extent_wkt") is not None
                            else None
                        ),
                        csw_record_jsonb=csw_record,
                        csw_raw_xml=str(row.get("xml_content") or "") or None,
                        schema_name=str(existing_layer.get("schema_name") or duckdb_schema),
                        table_name=str(existing_layer.get("table_name") or ""),
                        create_new_version=False,
                    )

                    skipped += 1
                    logger.info(
                        "data unchanged; refreshed metadata only resource_id=%s signature=%s",
                        resource_id,
                        signature,
                    )
                    continue

                logger.warning(
                    "data unchanged but catalog layer missing; reprocessing resource_id=%s",
                    resource_id,
                )

            _delete_failed_signature(
                metadata_conn,
                schema=schema,
                resource_id=resource_id,
                signature=signature,
            )

            version_num = _next_version(metadata_conn, resource_id, schema=schema)
            dataset_hash = _dataset_hash(package_id, resource_id)
            duck_table = f"ds_{dataset_hash}_v{version_num}"
            ext = _detect_ext(str(row["resource_url"]), row.get("resource_format"))

            version_id = str(uuid.uuid4())
            processed_at = datetime.now(timezone.utc)

            temp_path: str | None = None
            temp_dirs: list[str] = []
            cleanup_paths: set[str] = set()
            try:
                if _is_wfs_resource(row):
                    logger.info("resource_id=%s detected as wfs; downloading via goatlib", resource_id)
                    temp_path, wfs_temp_dir = _download_wfs_via_goatlib(str(row["resource_url"]))
                    temp_dirs.append(wfs_temp_dir)
                    wfs_ext = "parquet" if temp_path.endswith(".parquet") else "geojson"
                    load_path = temp_path
                    load_ext = wfs_ext
                else:
                    logger.info(
                        "resource_id=%s downloading from %s",
                        resource_id,
                        row["resource_url"],
                    )
                    temp_path = _download(str(row["resource_url"]))
                    cleanup_paths.add(temp_path)
                    load_path = temp_path
                    load_ext = ext

                if load_ext != "parquet":
                    convert_dir = tempfile.mkdtemp(prefix="ckan_convert_")
                    temp_dirs.append(convert_dir)
                    converted_path = _convert_to_goatlib_parquet(load_path, dest_dir=convert_dir)
                    cleanup_paths.add(converted_path)
                    load_path = converted_path
                    load_ext = "parquet"

                row_count, geometry_type, extent_wkt = _load_into_ducklake(
                    file_path=load_path,
                    table_schema=duckdb_schema,
                    table_name=duck_table,
                    ext=load_ext,
                    pg_host=metadata_pg_host,
                    pg_port=metadata_pg_port,
                    pg_db=metadata_pg_db,
                    pg_user=metadata_pg_user,
                    pg_password=resolved_metadata_pg_password,
                    ducklake_data_dir=ducklake_data_dir,
                    ducklake_catalog_schema=ducklake_catalog_schema,
                )
                logger.info(
                    "resource_id=%s loaded into ducklake table=%s rows=%s",
                    resource_id,
                    duck_table,
                    row_count,
                )

                with metadata_conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {schema}.processor_dataset_version(
                            id, run_id, package_id, resource_id, signature, version_num,
                            duckdb_path, duckdb_table, row_count, status, error, processed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)
                        """,
                        (
                            version_id,
                            run_id,
                            package_id,
                            resource_id,
                            signature,
                            version_num,
                            duckdb_path,
                            duck_table,
                            row_count,
                            "success",
                            processed_at,
                        ),
                    )

                csw_record = _build_csw_record(row)

                # Materialize canonical metadata into datacatalog.layer for APIs and GeoAPI pointer resolution.
                layer_name = str(csw_record.get("title") or resource_id)

                catalog_layer_id = _upsert_catalog_layer(
                    metadata_conn,
                    schema=schema,
                    resource_id=resource_id,
                    name=layer_name,
                    geometry_type=geometry_type,
                    extent_wkt=extent_wkt,
                    csw_record_jsonb=csw_record,
                    csw_raw_xml=str(row.get("xml_content") or "") or None,
                    schema_name=duckdb_schema,
                    table_name=duck_table,
                    version_num=version_num,
                    create_new_version=True,
                )
                logger.info(
                    "resource_id=%s upserted datacatalog.layer id=%s",
                    resource_id,
                    catalog_layer_id,
                )

                processed += 1
                logger.info(
                    "resource_id=%s processed successfully version=%s",
                    resource_id,
                    version_num,
                )
            except Exception as exc:
                failed += 1
                logger.exception(
                    "resource_id=%s failed to process version=%s error=%s",
                    resource_id,
                    version_num,
                    str(exc),
                )
                with metadata_conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {schema}.processor_dataset_version(
                            id, run_id, package_id, resource_id, signature, version_num,
                            duckdb_path, duckdb_table, row_count, status, error, processed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
                        """,
                        (
                            version_id,
                            run_id,
                            package_id,
                            resource_id,
                            signature,
                            version_num,
                            duckdb_path,
                            duck_table,
                            "failed",
                            str(exc),
                            processed_at,
                        ),
                    )
            finally:
                for path in cleanup_paths:
                    if os.path.exists(path):
                        os.remove(path)
                for temp_dir in temp_dirs:
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)

        summary = {"processed": processed, "skipped": skipped, "failed": failed, "candidates": len(candidates)}
        status = "failed" if failed > 0 else "success"
        logger.info(
            "sync completed run_id=%s status=%s summary=%s",
            run_id,
            status,
            json.dumps(summary, sort_keys=True),
        )

        metadata_conn.commit()

    return {
        "run_id": run_id,
        "duckdb_path": duckdb_path,
        **summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync CKAN API metadata resources to versioned DuckDB tables")
    parser.add_argument("--ckan-package-search-url", required=True)
    parser.add_argument("--ckan-api-key", default=None)
    parser.add_argument("--ckan-api-page-size", default=200, type=int)
    parser.add_argument("--ckan-api-max-pages", default=100, type=int)
    parser.add_argument("--meta-pg-host", required=True)
    parser.add_argument("--meta-pg-port", required=True, type=int)
    parser.add_argument("--meta-pg-db", required=True)
    parser.add_argument("--meta-pg-user", required=True)
    parser.add_argument("--meta-pg-password", default=None)
    parser.add_argument("--meta-pg-schema", default="datacatalog")
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--duckdb-schema", default="datacatalog")
    parser.add_argument("--ducklake-data-dir", default=os.getenv("DUCKLAKE_DATA_DIR", "/app/data/ducklake"), help="Shared DuckLake DATA_PATH")
    parser.add_argument("--ducklake-catalog-schema", default=os.getenv("DUCKLAKE_CATALOG_SCHEMA", "ducklake"), help="DuckLake METADATA_SCHEMA in PostgreSQL")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log verbosity level for sync progress output",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    result = run_sync(
        ckan_package_search_url=args.ckan_package_search_url,
        ckan_api_key=args.ckan_api_key,
        ckan_api_page_size=args.ckan_api_page_size,
        ckan_api_max_pages=args.ckan_api_max_pages,
        metadata_pg_host=args.meta_pg_host,
        metadata_pg_port=args.meta_pg_port,
        metadata_pg_db=args.meta_pg_db,
        metadata_pg_user=args.meta_pg_user,
        metadata_pg_password=args.meta_pg_password,
        metadata_pg_schema=args.meta_pg_schema,
        duckdb_path=args.duckdb_path,
        duckdb_schema=args.duckdb_schema,
        ducklake_data_dir=args.ducklake_data_dir,
        ducklake_catalog_schema=args.ducklake_catalog_schema,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
