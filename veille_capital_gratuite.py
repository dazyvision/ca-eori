#!/usr/bin/env python3
"""Veille autonome BODACC + presse, sans Pappers ni IA payante (Python 3.10+).

python veille_capital_gratuite.py
python veille_capital_gratuite.py --input inputs/entreprises.csv --days 30
python veille_capital_gratuite.py --days 7 --max-age 5 --no-rss

CSV optionnel : colonne siren/SIREN obligatoire, séparateur auto-détecté.
Le CSV restreint le BODACC aux SIREN fournis. La presse sans correspondance
juridique exacte reste en revue. Aucun nom ambigu n'est attribué à un SIREN.
Les sorties sont indépendantes du vivier existant : aucun écrasement métier.
Modification de capital != augmentation != levée. Aucun avantage temporel garanti.
Conserver le dossier de sortie entre les runs pour le cache et la déduplication.
"""
import argparse
import csv
import datetime as dt
import email.utils
import gzip
import hashlib
import html
import json
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import xml.parsers.expat as expat
from pathlib import Path
from zoneinfo import ZoneInfo

BODACC = 'https://www.bodacc.fr/api/explore/v2.1/catalog/datasets/annonces-commerciales/records'
ENTREPRISES = 'https://recherche-entreprises.api.gouv.fr/search'
RSS = ['https://www.maddyness.com/feed/', 'https://www.frenchweb.fr/feed', 'https://www.usine-digitale.fr/rss']
FIELDS = ['event_id', 'siren', 'company', 'signal', 'date_publication', 'date_creation', 'code_naf', 'ca', 'ca_year', 'montant_annonce', 'source', 'source_url', 'preuve', 'statut', 'nouveau']


def get(url, params=None):
    if params:
        url += '?' + urllib.parse.urlencode(params)
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'VeilleCapitalGratuite/1.0', 'Accept-Encoding': 'gzip'})
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
            if raw[:2] == b'\x1f\x8b':
                raw = gzip.decompress(raw)
            return raw
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)


def normalize(value):
    value = ''.join(c for c in unicodedata.normalize('NFD', value) if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9]', '', value.lower())


def atomic_json(path, obj):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


def load_json(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def read_sirens(path):
    if not path:
        return None
    text = Path(path).read_text(encoding='utf-8-sig')
    try:
        dialect = csv.Sniffer().sniff(text[:8192], delimiters=',;\t')
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    column = next((k for k in reader.fieldnames or [] if k.strip().lower() == 'siren'), None)
    if not column:
        raise ValueError('CSV : colonne siren obligatoire')
    result = set()
    for line, row in enumerate(reader, 2):
        siren = re.sub(r'\s', '', row.get(column) or '')
        if not re.fullmatch(r'\d{9}', siren):
            raise ValueError(f'SIREN invalide ligne {line} : {siren!r}')
        result.add(siren)
    if not result:
        raise ValueError('CSV sans SIREN')
    return result


def resolve(query, cache, today):
    cached = cache.get(query)
    if cached and cached.get('checked') == today.isoformat():
        return cached['result']
    time.sleep(.35)
    data = json.loads(get(ENTREPRISES, {'q': query, 'per_page': 25}))
    exact = {str(e['siren']): e for e in data.get('results', []) if
             (str(e.get('siren')) == query if re.fullmatch(r'\d{9}', query) else
              normalize(e.get('nom_raison_sociale') or e.get('nom_complet') or '') == normalize(query))}
    result = next(iter(exact.values())) if len(exact) == 1 else None
    cache[query] = {'checked': today.isoformat(), 'result': result}
    return result


def classify(text):
    text = normalize(text)
    if 'reduction' in text or 'diminution' in text:
        return 'CAPITAL_A_REVOIR'
    if 'augmentation' in text or 'capitalaugmente' in text:
        return 'AUGMENTATION_MENTIONNEE'
    return 'MODIFICATION_CAPITAL_A_VERIFIER'


def bodacc_events(since, today, allowed):
    # Requête large puis qualification locale : ne pas confondre toute modification avec une hausse.
    where = f'dateparution >= "{since}" AND dateparution <= "{today}" AND search(modificationsgenerales,"capital")'
    offset = 0
    while True:
        data = json.loads(get(BODACC, {'where': where, 'order_by': 'dateparution ASC,id ASC', 'limit': 100, 'offset': offset}))
        records = data.get('results', [])
        for r in records:
            raw = r.get('modificationsgenerales') or ''
            try:
                decoded = json.loads(raw) if isinstance(raw, str) else raw
                evidence = decoded.get('descriptif', '')
            except (ValueError, AttributeError):
                evidence = str(raw)
            if 'capital' not in normalize(evidence):
                continue
            registers = r.get('registre') or []
            if isinstance(registers, str):
                registers = [registers]
            sirens = {re.sub(r'\s', '', str(s)) for s in registers}
            sirens = {s for s in sirens if re.fullmatch(r'\d{9}', s)}
            if allowed is not None:
                sirens &= allowed
            for siren in sirens:
                yield {'event_id': f"bodacc:{r['id']}:{siren}", 'siren': siren,
                       'company': r.get('commercant', ''), 'signal': classify(evidence),
                       'date_publication': r['dateparution'], 'source': 'BODACC',
                       'source_url': r.get('url_complete') or 'https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:' + r['id'],
                       'preuve': evidence}
        offset += len(records)
        if offset >= data.get('total_count', 0) or not records:
            break
        if offset >= 10000:
            raise RuntimeError('Fenêtre BODACC trop large pour la pagination : réduire --days (aucune couverture complète garantie)')


def rss_events(since, warnings):
    for url in RSS:
        try:
            root = parse_rss(get(url), url)
            for item in root.findall('.//item'):
                title = html.unescape(item.findtext('title', ''))
                description = re.sub('<[^>]+>', ' ', html.unescape(item.findtext('description', '')))
                text = title + ' ' + description
                if not re.search(r'\bl[eè]ve\b|levée de fonds|tour de table|boucle.{0,30}millions?', text, re.I):
                    continue
                try:
                    date = email.utils.parsedate_to_datetime(item.findtext('pubDate', '')).date()
                except (ValueError, TypeError):
                    warnings.append(f'RSS sans date valide : {title}')
                    continue
                if not since <= date <= dt.datetime.now(ZoneInfo('Europe/Paris')).date():
                    continue
                match = re.match(r"^(?:La startup |La start-up )?(.{2,70}?)\s+(?:lève|boucle|annonce)\b", title, re.I)
                name = match.group(1).strip() if match else ''
                link = item.findtext('link', '')
                amount = re.search(r'\d+(?:[,.]\d+)?\s*(?:millions? d[’\x27]euros|M€)', text, re.I)
                yield {'event_id': 'rss:' + hashlib.sha256((link or title).encode()).hexdigest(),
                       'company': name, 'signal': 'ANNONCE_PRESSE_A_VERIFIER', 'date_publication': date.isoformat(),
                       'source': 'RSS', 'source_url': link, 'preuve': text[:2000],
                       'montant_annonce': amount.group(0) if amount else ''}
        except Exception as exc:
            warnings.append(f'Flux {url} : {type(exc).__name__}: {exc}')


def qualify(event, ent, today, max_age, ca_max, allowed):
    if not ent:
        return {**event, 'statut': 'IDENTITE_NON_RESOLUE'}
    if allowed is not None and str(ent['siren']) not in allowed:
        return None
    if ent.get('etat_administratif') != 'A':
        return None
    try:
        creation = dt.date.fromisoformat(ent['date_creation'])
    except (KeyError, ValueError, TypeError):
        return {**event, 'siren': ent['siren'], 'statut': 'DATE_CREATION_INCONNUE'}
    try:
        cutoff = today.replace(year=today.year - max_age)
    except ValueError:
        cutoff = today.replace(year=today.year - max_age, day=28)
    if not cutoff < creation <= today:
        return None
    finances = ent.get('finances') or {}
    year = max(finances, default='')
    ca = (finances.get(year) or {}).get('ca')
    if ca is not None and float(ca) > ca_max:
        return None
    return {**event, 'siren': ent['siren'], 'company': ent.get('nom_raison_sociale') or ent.get('nom_complet') or event['company'],
            'date_creation': creation.isoformat(), 'code_naf': ent.get('activite_principale', ''),
            'ca': ca if ca is not None else '', 'ca_year': year,
            'statut': 'A_QUALIFIER' if event['source'] == 'BODACC' else 'PRESSE_IDENTITE_VERIFIEE_LEVEE_A_CONFIRMER'}



def parse_rss(raw, url):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        parser = expat.ParserCreate()
        try:
            parser.Parse(raw, True)
        except expat.ExpatError as exc:
            if exc.code != expat.errors.codes[expat.errors.XML_ERROR_JUNK_AFTER_DOC_ELEMENT]:
                raise
            root = ET.fromstring(raw[:parser.ErrorByteIndex])
            print(f"INFO : suffixe parasite ignoré pour {url}")
        else:
            raise ValueError("XML non interprétable")
    if root.tag != "rss" or root.find("channel") is None:
        raise ValueError("Réponse non RSS")
    return root


# Heuristiques de présélection, jamais preuve d'innovation ou d'indépendance.
TECH_CODES = ("58.2", "62.01", "63.11", "72.", "21.", "26.", "27.", "28.", "32.5")
SECONDARY_CODES = ("64", "65", "66", "68", "70.10")
NEW_FIELDS = FIELDS + ["lot", "motif", "profil", "ca_statut", "premiere_detection",
    "derniere_detection", "nb_augmentations", "gouvernance_mentionnee", "nature_operation",
    "capital_avant", "capital_apres", "variation_nominale", "variation_pct",
    "montant_statut", "statut_presse", "presse_urls", "date_premiere_presse",
    "delai_presse_jours", "sources_bodacc", "verification_activite"]


def operation_nature(text):
    t = normalize(text)
    kinds = []
    for token, label in (("numeraire", "NUMERAIRE"), ("primedemission", "PRIME_EMISSION_MENTIONNEE"),
                         ("incorporationdereserves", "INCORPORATION_RESERVES"),
                         ("apportennature", "APPORT_NATURE"), ("conversion", "CONVERSION_A_VERIFIER"),
                         ("fusion", "FUSION_A_VERIFIER")):
        if token in t:
            kinds.append(label)
    return ";".join(kinds) or "NON_PRECISEE"


def explicit_amounts(text):
    # Only an explicit before/after statement in the SAME source is measured.
    # Neither current capital alone nor two unrelated historical snapshots suffice.
    number = r"([0-9][0-9 \u00a0\u202f]*(?:[,.][0-9]{1,2})?)"
    m = re.search(r"capital.{0,80}?(?:de|du)\s+" + number +
                  r"\s*(?:€|euros?|EUR)\s+(?:à|a|au)\s+" + number +
                  r"\s*(?:€|euros?|EUR)", text, re.I)
    if not m:
        return {}
    def amount(v):
        return float(re.sub(r"[ \u00a0\u202f]", "", v).replace(",", "."))
    before, after = map(amount, m.groups())
    return {"capital_avant": before, "capital_apres": after,
            "variation_nominale": after-before,
            "variation_pct": round(100*(after-before)/before, 2) if before else "",
            "montant_statut": "VARIATION_NOMINALE_EXPLICITE_PAS_MONTANT_LEVE"}


def write_rows(path, rows):
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=NEW_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Détection BODACC prioritaire, profils jeunes, presse en confirmation. Sans Pappers.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--max-age", type=int, default=8)
    parser.add_argument("--ca-max", type=float, default=15000000)
    parser.add_argument("--input", help="CSV facultatif de SIREN à surveiller")
    parser.add_argument("--no-rss", action="store_true")
    parser.add_argument("--output-dir", default="sorties/veille_gratuite")
    parser.add_argument("--replay", help="Reclasser un ancien CSV sans appels BODACC ni résolution")
    parser.add_argument("--nominal-min", type=float, default=50000,
                        help="Seuil de variation nominale pour priorité, PAS seuil de fonds levés")
    args = parser.parse_args(argv)
    if not 1 <= args.days <= 90 or not 1 <= args.max_age <= 100 or args.ca_max <= 0 or args.nominal_min < 0:
        parser.error("Paramètres hors limites")
    today = dt.datetime.now(ZoneInfo("Europe/Paris")).date()
    stamp = today.isoformat()
    since = today - dt.timedelta(days=args.days-1)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    state = load_json(out / "startup_state.json")
    cache = load_json(out / "entreprises_cache.json")
    allowed = read_sirens(args.input)
    warnings, collected, press = [], [], []
    fatal = False
    if args.replay:
        with open(args.replay, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                (press if row.get("source") == "RSS" else collected).append(row)
        print("REPLAY : ancien périmètre du CSV conservé ; pas de rattrapage des 5-8 ans")
    else:
        try:
            for event in bodacc_events(since.isoformat(), stamp, allowed):
                try:
                    ent = resolve(event["siren"], cache, today)
                    row = qualify(event, ent, today, args.max_age, args.ca_max, allowed)
                except Exception as exc:
                    row = {**event, "statut": "ERREUR_RESOLUTION_A_RETENTER"}
                    warnings.append(f"Résolution {event['siren']} : {type(exc).__name__}")
                if row:
                    collected.append(row)
        except Exception as exc:
            fatal = True
            warnings.append(f"BODACC incomplet : {type(exc).__name__}: {exc}")
        if not args.no_rss:
            press.extend(rss_events(since, warnings))
    # RSS never creates a prospect. Match only exact legal identity to an existing candidate.
    # Reuse prior candidates so a later press publication can confirm them.
    for row in collected:
        key = row.get("siren")
        if not key:
            continue
        candidate = state.setdefault(key, {"first_seen": stamp, "events": {}, "press": {}})
        candidate["last_seen"] = stamp
        candidate["events"][row["event_id"]] = row
    for article in press:
        key = article.get("siren") or ""
        if not key and not args.replay and article.get("company"):
            try:
                ent = resolve(article["company"], cache, today)
                key = str(ent["siren"]) if ent else ""
            except Exception as exc:
                warnings.append(f"Presse identité {article['company']} : {type(exc).__name__}")
        if key in state and (allowed is None or key in allowed):
            state[key]["press"][article["event_id"]] = article
    rows = []
    cutoff = today.replace(year=today.year-args.max_age, day=28) if today.month == 2 and today.day == 29 else today.replace(year=today.year-args.max_age)
    for siren, candidate in state.items():
        if allowed is not None and siren not in allowed:
            continue
        events = list(candidate["events"].values())
        increases = [e for e in events if e.get("signal") == "AUGMENTATION_MENTIONNEE"]
        base = max(increases or events, key=lambda e: e.get("date_publication", ""))
        row = dict(base)
        try:
            creation = dt.date.fromisoformat(row.get("date_creation", ""))
            if not cutoff < creation <= today:
                continue
        except ValueError:
            pass
        if row.get("ca") not in (None, ""):
            try:
                if float(row["ca"]) > args.ca_max:
                    continue
            except ValueError:
                row["statut"] = "CA_INVALIDE_A_VERIFIER"
        naf = row.get("code_naf", "")
        secondary = naf.startswith(SECONDARY_CODES)
        tech = naf.startswith(TECH_CODES)
        operation = operation_nature(" ".join(e.get("preuve", "") for e in increases))
        amounts = explicit_amounts(base.get("preuve", ""))
        press_items = list(candidate["press"].values())
        verified = row.get("statut") in ("A_QUALIFIER", "PRESSE_IDENTITE_VERIFIEE_LEVEE_A_CONFIRMER")
        accounting = any(t in operation for t in ("INCORPORATION_RESERVES", "APPORT_NATURE", "CONVERSION", "FUSION"))
        nominal_large = amounts.get("variation_nominale", 0) >= args.nominal_min
        if not verified:
            lot, reason = "REVUE", "Identité, âge ou données à vérifier"
        elif not increases:
            lot, reason = "AUTRES", "Pas d'augmentation explicitement mentionnée"
        elif secondary or accounting:
            lot, reason = "SECONDAIRE", "Structure patrimoniale/financière possible ou nature opération à examiner"
        elif press_items:
            lot, reason = "MEDIATISE", "Candidat BODACC avec correspondance presse exacte"
        elif tech or nominal_large or "NUMERAIRE" in operation:
            lot, reason = "PRIORITAIRE", "Augmentation explicite + " + ("profil métier ciblé" if tech else "variation nominale importante" if nominal_large else "numéraire mentionné")
        else:
            lot, reason = "REVUE", "Augmentation explicite ; activité et financement à préciser"
        first_press = min((p.get("date_publication", "") for p in press_items), default="")
        delay = ""
        if first_press and increases:
            delay = (dt.date.fromisoformat(first_press) - min(dt.date.fromisoformat(e["date_publication"]) for e in increases)).days
        row.update(amounts)
        row.update(lot=lot, motif=reason, profil="CODE_METIER_CIBLE_A_CONFIRMER" if tech else "STRUCTURE_A_EXAMINER" if secondary else "ACTIVITE_A_VERIFIER",
                   ca_statut="PUBLIE" if row.get("ca") not in (None, "") else "INCONNU",
                   premiere_detection=candidate["first_seen"], derniere_detection=candidate["last_seen"],
                   nb_augmentations=len(increases), gouvernance_mentionnee="OUI" if any("administration" in normalize(e.get("preuve", "")) for e in increases) else "NON_MENTIONNEE",
                   nature_operation=operation, montant_statut=amounts.get("montant_statut", "MONTANT_INCONNU"),
                   statut_presse="CORRESPONDANCE_TROUVEE" if press_items else "NON_VERIFIEE_CE_RUN" if args.no_rss else "COUVERTURE_INCOMPLETE" if warnings else "AUCUNE_CORRESPONDANCE_DANS_FLUX_CONSULTES",
                   presse_urls=" ; ".join(p.get("source_url", "") for p in press_items), date_premiere_presse=first_press,
                   delai_presse_jours=delay, sources_bodacc=" ; ".join(dict.fromkeys(e.get("source_url", "") for e in events)),
                   verification_activite="A_FAIRE_SUR_SITE_OFFICIEL", nouveau="OUI" if candidate["first_seen"] == stamp else "NON")
        rows.append(row)
    order = {"PRIORITAIRE": 0, "REVUE": 1, "MEDIATISE": 2, "SECONDAIRE": 3, "AUTRES": 4}
    rows.sort(key=lambda r: (order[r["lot"]], r.get("company", "")))
    write_rows(out / f"veille_{stamp}.csv", rows)
    for lot in order:
        write_rows(out / f"startup_{lot.lower()}_{stamp}.csv", [r for r in rows if r["lot"] == lot])
    lines = [f"# Détection BODACC startups - {stamp}", "", f"{len(rows)} entreprises dans la mémoire exportée", "",
             "Le lot prioritaire est une présélection à vérifier, pas une levée confirmée ni une preuve d'absence de presse.",
             "Montants : variation nominale uniquement si avant/après explicites dans la même annonce ; prime et fonds levés non déduits.",
             "Les actes INPI, sites officiels, recrutements et investisseurs ne sont pas consultés automatiquement dans cette version.",
             "Le délai presse se mesure depuis la publication BODACC, pas depuis la première détection. Les RSS ne couvrent pas toutes les archives.", ""]
    for lot in order:
        subset = [r for r in rows if r["lot"] == lot]
        lines.append(f"## {lot} : {len(subset)}")
        for r in subset:
            lines.append(f"- {r.get('company')} ({r['siren']}) : {r['motif']} | {r['source_url']}")
    if warnings:
        lines += ["", "## Couverture incomplète"] + ["- " + w for w in warnings]
    (out / f"veille_{stamp}.md").write_text("\n".join(lines), encoding="utf-8")
    atomic_json(out / "startup_state.json", state)
    atomic_json(out / "entreprises_cache.json", cache)
    print(f"{len(rows)} entreprises exportées : {out}")
    for lot in order:
        print(f"{lot} : {sum(r['lot']==lot for r in rows)}")
    for w in warnings:
        print("ATTENTION : " + w)
    return 2 if fatal or warnings else 0


if __name__ == "__main__":
    raise SystemExit(main())
