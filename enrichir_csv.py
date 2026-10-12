#!/usr/bin/env python3
"""Enrich a CSV of French SIRENs with public turnover and EU EORI checks.

Turnover is taken from the open API Recherche d'entreprises (DINUM), listed
on data.gouv.fr. It is only returned when public financial information is
available. An absent turnover is not zero and is not an estimate.

EORI is checked through the European Commission validation SOAP service.
An EORI result is evidence of customs registration only; it does not establish
current trade, currency flows, volumes, or an FX need. Network/service errors
are kept distinct from a negative validation.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests

RECHERCHE_API = "https://recherche-entreprises.api.gouv.fr/search"
EORI_WS = "https://ec.europa.eu/taxation_customs/dds2/eos/validation/services/validation"
CA_SOURCE = "https://annuaire-entreprises.data.gouv.fr/donnees/api-entreprises"
EORI_SOURCE = "https://taxation-customs.ec.europa.eu/customs/customs-procedures/import-and-export/customs-operations/economic-operators-registration-and-identification-number-eori_en"
PAUSE_SECONDS = 0.4

SOAP_ENVELOPE = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:eos="http://eori.ws.eos.dds.s/">
  <soapenv:Header/>
  <soapenv:Body>
    <eos:validateEORI>
      <eos:eori>{eori}</eos:eori>
    </eos:validateEORI>
  </soapenv:Body>
</soapenv:Envelope>"""

OUTPUT_FIELDS = (
    "ca_eur", "ca_exercice", "ca_statut", "ca_source", "ca_verifie_le",
    "eori", "eori_statut", "eori_source", "eori_verifie_le",
)


def normalize_siren(value: str | None) -> str:
    """Return a nine-digit SIREN or an empty string for an invalid value."""
    digits = re.sub(r"\D", "", value or "")
    return digits if len(digits) == 9 else ""


def _ca_from_finances(finances: Any) -> tuple[str, str] | None:
    """Extract the newest disclosed turnover from known API finance shapes."""
    entries: list[tuple[int, Any]] = []
    if isinstance(finances, dict):
        for year, values in finances.items():
            try:
                entries.append((int(str(year)[:4]), values))
            except (TypeError, ValueError):
                continue
    elif isinstance(finances, list):
        for values in finances:
            if not isinstance(values, dict):
                continue
            year = values.get("annee") or values.get("exercice") or values.get("date_fin_exercice")
            try:
                entries.append((int(str(year)[:4]), values))
            except (TypeError, ValueError):
                continue

    for year, values in sorted(entries, key=lambda item: item[0], reverse=True):
        if not isinstance(values, dict):
            continue
        for key in ("ca", "chiffre_affaires", "chiffre_d_affaires", "chiffre_affaire"):
            value = values.get(key)
            if value is not None and value != "":
                return str(value), str(year)
    return None


def fetch_turnover(session: requests.Session, siren: str) -> tuple[str, str, str]:
    """Return (turnover_eur, year, status) using the public DINUM search API."""
    try:
        response = session.get(
            RECHERCHE_API,
            params={"q": siren, "per_page": 10},
            headers={"Accept": "application/json", "User-Agent": "rev-os-csv-enrichment/1.0"},
            timeout=25,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return "", "", "ERREUR_API"

    results = payload.get("results", []) if isinstance(payload, dict) else []
    exact = next((r for r in results if isinstance(r, dict) and normalize_siren(str(r.get("siren", ""))) == siren), None)
    if exact is None:
        return "", "", "ENTREPRISE_NON_TROUVEE"

    financials = _ca_from_finances(exact.get("finances"))
    if financials is None:
        return "", "", "CA_NON_PUBLIE"
    amount, year = financials
    return amount, year, "CA_PUBLIE"


def _local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def fetch_eori(session: requests.Session, siren: str) -> tuple[str, str]:
    """Return (EORI number, status) with service failures kept explicit."""
    number = f"FR{siren}"
    try:
        response = session.post(
            EORI_WS,
            data=SOAP_ENVELOPE.format(eori=number).encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": '""',
                "User-Agent": "rev-os-csv-enrichment/1.0",
                "Accept": "text/xml, application/soap+xml",
            },
            timeout=25,
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except (requests.RequestException, ET.ParseError, ValueError):
        return number, "ERREUR_SERVICE"

    fields = {_local_tag(node.tag): (node.text or "").strip() for node in root.iter()}
    fault = fields.get("faultstring")
    if fault:
        return number, "ERREUR_SERVICE"

    description = fields.get("statusDescr", "").lower()
    code = fields.get("status", "")
    if "not valid" in description or "invalid" in description:
        return number, "NON_VALIDE"
    if "valid" in description or code == "0":
        return number, "VALIDE"
    return number, "ERREUR_REPONSE_INCONNUE"


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str], str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("CSV vide ou sans en-tête")
        siren_column = next((name for name in reader.fieldnames if name and name.strip().lower() in {"siren", "siren_number", "siren number"}), None)
        if not siren_column:
            raise ValueError("Colonne SIREN introuvable (en-tête attendu : siren)")
        return list(reader), list(reader.fieldnames), delimiter


def enrich(input_path: Path, output_path: Path, pause: float = PAUSE_SECONDS) -> dict[str, int]:
    if input_path.resolve() == output_path.resolve():
        raise ValueError("Le CSV de sortie doit être différent du fichier source")
    rows, fields, delimiter = read_csv(input_path)
    siren_column = next(name for name in fields if name and name.strip().lower() in {"siren", "siren_number", "siren number"})
    out_fields = fields + [name for name in OUTPUT_FIELDS if name not in fields]
    checked_at = dt.date.today().isoformat()
    ca_session = requests.Session()
    eori_session = requests.Session()
    turnover_cache: dict[str, tuple[str, str, str]] = {}
    eori_cache: dict[str, tuple[str, str]] = {}
    totals = {"rows": len(rows), "ca_publie": 0, "eori_valide": 0, "erreurs": 0, "siren_invalide": 0}

    with ThreadPoolExecutor(max_workers=2) as pool:
        for row in rows:
            siren = normalize_siren(row.get(siren_column))
            if not siren:
                row.update({"ca_eur": "", "ca_exercice": "", "ca_statut": "SIREN_INVALIDE", "ca_source": CA_SOURCE, "ca_verifie_le": checked_at,
                            "eori": "", "eori_statut": "SIREN_INVALIDE", "eori_source": EORI_SOURCE, "eori_verifie_le": checked_at})
                totals["siren_invalide"] += 1
                continue

            if siren not in turnover_cache:
                ca_future = pool.submit(fetch_turnover, ca_session, siren)
                eori_future = pool.submit(fetch_eori, eori_session, siren)
                turnover_cache[siren] = ca_future.result()
                eori_cache[siren] = eori_future.result()
                # One request per service per interval. Parallel calls keep
                # the per-service rate at or below 1 / pause seconds.
                time.sleep(pause)

            amount, year, ca_status = turnover_cache[siren]
            row.update({"ca_eur": amount, "ca_exercice": year, "ca_statut": ca_status, "ca_source": CA_SOURCE, "ca_verifie_le": checked_at})
            if ca_status == "CA_PUBLIE":
                totals["ca_publie"] += 1
            elif ca_status == "ERREUR_API":
                totals["erreurs"] += 1

            number, eori_status = eori_cache[siren]
            row.update({"eori": number, "eori_statut": eori_status, "eori_source": EORI_SOURCE, "eori_verifie_le": checked_at})
            if eori_status == "VALIDE":
                totals["eori_valide"] += 1
            elif eori_status.startswith("ERREUR"):
                totals["erreurs"] += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=out_fields, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrichit un CSV SIREN avec CA public et validation EORI")
    parser.add_argument("--input", required=True, type=Path, help="CSV existant contenant une colonne siren")
    parser.add_argument("--output", required=True, type=Path, help="nouveau CSV enrichi")
    parser.add_argument("--pause", type=float, default=PAUSE_SECONDS, help="pause entre les appels (secondes)")
    args = parser.parse_args()
    if args.pause < 0:
        parser.error("--pause doit être positif ou nul")
    try:
        totals = enrich(args.input, args.output, args.pause)
    except (OSError, ValueError) as exc:
        print(f"Erreur: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(totals, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
