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
            root = ET.fromstring(get(url))
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--input', help='CSV de SIREN à surveiller ; absent = découverte France')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--max-age', type=int, default=5)
    parser.add_argument('--ca-max', type=float, default=15_000_000, help='CA publié maximum ; inconnu conservé')
    parser.add_argument('--no-rss', action='store_true')
    parser.add_argument('--output-dir', default='sorties/veille_gratuite')
    args = parser.parse_args(argv)
    if args.days < 1 or args.max_age < 1 or args.ca_max <= 0:
        parser.error('days, max-age et ca-max doivent être positifs')
    today = dt.datetime.now(ZoneInfo('Europe/Paris')).date()
    since = today - dt.timedelta(days=args.days - 1)
    allowed = read_sirens(args.input)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    cache = load_json(out / 'entreprises_cache.json')
    seen = load_json(out / 'evenements_vus.json')
    warnings, rows = [], []
    fatal = False
    events = []
    try:
        events.extend(bodacc_events(since.isoformat(), today.isoformat(), allowed))
    except Exception as exc:
        fatal = True
        warnings.append(f'BODACC incomplet : {type(exc).__name__}: {exc}')
    if not args.no_rss:
        events.extend(rss_events(since, warnings))
    unique = {e['event_id']: e for e in events}
    for event in unique.values():
        try:
            query = event.get('siren') or event.get('company')
            ent = resolve(query, cache, today) if query else None
            row = qualify(event, ent, today, args.max_age, args.ca_max, allowed)
        except Exception as exc:
            warnings.append(f"Résolution {event.get('siren') or event['company']} : {type(exc).__name__}: {exc}")
            row = {**event, 'statut': 'ERREUR_RESOLUTION_A_RETENTER'}
        if row:
            row['nouveau'] = 'NON' if row['event_id'] in seen else 'OUI'
            rows.append(row)
    stamp = today.isoformat()
    csv_path = out / f'veille_{stamp}.csv'
    with csv_path.open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    lines = [f'# Veille capital - {stamp}', '', f'Fenêtre : {since} au {today} | moins de {args.max_age} ans | zéro appel Pappers', '',
             'Une modification ne prouve ni augmentation ni levée. Une annonce BODACC peut précéder ou suivre la presse.', '',
             f'{len(rows)} événements retenus, dont {sum(r["nouveau"] == "OUI" for r in rows)} nouveaux.', '']
    for row in rows:
        lines.extend([f"- {row.get('company') or 'Identité à vérifier'} | {row.get('siren', '')} | {row['signal']} | {row['statut']} | nouveau={row['nouveau']}",
                      f"  {row['source_url']}", f"  {row['preuve'][:600]}"])
    if warnings:
        lines += ['', '## Couverture partielle / erreurs', ''] + ['- ' + w for w in warnings]
    (out / f'veille_{stamp}.md').write_text('\n'.join(lines), encoding='utf-8')
    atomic_json(out / 'entreprises_cache.json', cache)
    # Les cas non résolus restent nouveaux pour être retentés au prochain passage.
    for row in rows:
        if row['statut'] not in ('IDENTITE_NON_RESOLUE', 'ERREUR_RESOLUTION_A_RETENTER', 'DATE_CREATION_INCONNUE'):
            seen[row['event_id']] = stamp
    atomic_json(out / 'evenements_vus.json', seen)
    print(f'{len(rows)} événements exportés : {csv_path}')
    for warning in warnings:
        print('ATTENTION : ' + warning)
    return 2 if fatal or warnings else 0


if __name__ == '__main__':
    raise SystemExit(main())
