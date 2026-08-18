#!/usr/bin/env python3
"""
Genera stage-specs.json leggendo le sorgenti Scala.

Il manifest e' cio' che il catalogo della piattaforma consuma: se lo si scrive a mano, diverge dal
codice al primo stage aggiunto e nessuno se ne accorge, perche' un catalogo incompleto non da'
errore — semplicemente non mostra nulla. Qui invece la fonte e' il codice stesso.

Estrae, per ogni classe che dichiara `override def name`:
  - nome dello stage
  - descrizione: la prima frase dello scaladoc
  - guida d'uso: il blocco "Pipeline YAML" dello scaladoc, che gia' documenta gli argomenti
  - schema argomenti: le chiamate `args.<tipo>(indice, default)`, con il nome della val come nome
    dell'argomento — e' l'unica etichetta significativa disponibile, gli argomenti dell'SDK sono
    POSIZIONALI e non hanno nome proprio.
"""
import json, re, sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src/main/scala"

nome_re   = re.compile(r'override\s+def\s+name:\s*String\s*=\s*"([^"]+)"')
arg_re    = re.compile(r'(?:val\s+(\w+)\s*=\s*)?args\.(string|int|double)\(\s*(\d+)\s*,\s*([^)]*?)\s*\)')
doc_re    = re.compile(r'/\*\*(.*?)\*/', re.S)

def pulisci(doc):
    # Toglie il margine dello scaladoc ("   * ") ma CONSERVA il rientro successivo: la guida d'uso
    # e' YAML, e in YAML il rientro e' sintassi, non decorazione.
    return [re.sub(r'^\s*\* ?', '', r).rstrip() for r in doc.split('\n')]

def estrai(path):
    testo = path.read_text(encoding='utf-8')
    m = nome_re.search(testo)
    if not m:
        return None
    stage = m.group(1)

    # lo scaladoc immediatamente precedente alla classe
    doc = ''
    docs = list(doc_re.finditer(testo))
    if docs:
        doc = docs[0].group(1)
    righe = pulisci(doc)

    # descrizione = testo fino alla riga vuota
    desc = []
    for r in righe:
        if not r.strip():
            if desc: break
            continue
        if r.strip().startswith('Pipeline YAML'): break
        desc.append(r.strip())
    descrizione = ' '.join(desc).strip() or None

    # guida = blocco {{{ ... }}}
    guida = None
    blocco = re.search(r'\{\{\{(.*?)\}\}\}', doc, re.S)
    if blocco:
        guida = '\n'.join(pulisci(blocco.group(1))).strip() or None

    # commenti per-argomento dal blocco YAML, in ordine
    commenti = []
    if blocco:
        for r in pulisci(blocco.group(1)):
            c = re.search(r'#\s*(.+)$', r)
            if c and '- ' in r:
                commenti.append(c.group(1).strip())

    # argomenti: ultima occorrenza vince per indice (le prime possono stare in rami alternativi)
    per_indice = {}
    for a in arg_re.finditer(testo):
        val, tipo, idx, default = a.group(1), a.group(2), int(a.group(3)), a.group(4).strip()
        if idx in per_indice and per_indice[idx].get('name') and not val:
            continue
        # Solo i LETTERALI diventano un default dichiarato. Alcuni stage calcolano il valore di
        # ripiego con un'espressione (es. `if (mode == "timelinetone") "tone" else "volume"`):
        # riportarla come default sarebbe falso, e troncata al primo `)` sarebbe pure illeggibile.
        letterale = None
        if default.startswith('"') and default.endswith('"') and default.count('"') == 2:
            letterale = default.strip('"')
        elif re.fullmatch(r'-?\d+(\.\d+)?', default):
            letterale = default
        voce = {'name': val or f'arg{idx}',
                'type': 'string' if tipo == 'string' else 'number',
                'required': letterale in (None, '')}
        if letterale not in (None, ''):
            voce['default'] = letterale
        if idx < len(commenti):
            voce['description'] = commenti[idx]
        per_indice[idx] = voce

    schema = [per_indice[i] for i in sorted(per_indice)]
    spec = {'stage_name': stage, 'aliases': [], 'arg_schema': schema}
    if descrizione: spec['description'] = descrizione
    if guida: spec['usage_guide'] = guida
    return spec

# Stage di impalcatura per collaudare il ponte RDD dell'SDK. Restano nel jar — servono a chi
# sviluppa un plugin — ma NON vanno pubblicati: nel catalogo di un tenant sarebbero rumore accanto
# a fonti dati vere. Toglierli da qui e' la riga da cancellare se un domani servissero.
NON_PUBBLICATI = {'rowMultiply', 'filterGt', 'explodeCsv', 'sumByKey', 'countByKey'}

def main():
    specs = []
    for p in sorted(SRC.rglob('*.scala')):
        testo = p.read_text(encoding='utf-8')
        # un file puo' dichiarare piu' stage (RddDemoStages)
        if testo.count('override def name') > 1:
            for m in nome_re.finditer(testo):
                specs.append({'stage_name': m.group(1), 'aliases': [], 'arg_schema': []})
            continue
        s = estrai(p)
        if s: specs.append(s)
    esclusi = [s['stage_name'] for s in specs if s['stage_name'] in NON_PUBBLICATI]
    specs = [s for s in specs if s['stage_name'] not in NON_PUBBLICATI]
    specs.sort(key=lambda s: s['stage_name'])
    if esclusi:
        print(f"non pubblicati (impalcatura di collaudo): {', '.join(sorted(esclusi))}", file=sys.stderr)
    print(json.dumps(specs, indent=2, ensure_ascii=False))
    print(f"{len(specs)} stage", file=sys.stderr)

main()
