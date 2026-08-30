#!/usr/bin/env python3
"""
extract_tropo_profile.py — pipeline GRIB2 (25-29/08/2026, operatore+assistente)

Estrae il profilo verticale (temperatura + quota geopotenziale) su TUTTI i
livelli di pressione isobarici disponibili in GFS e ECMWF Open Data, al
punto griglia più vicino a Castel Volturno (41.035N, 13.942E) — non più un
solo istante (fxx=0), ma una SERIE oraria/trioraria che copre la finestra
di previsione 0-12h del pannello, con margine fino a 24h per coprire
qualunque momento di lancio tra un ciclo cron e il successivo (vedi FIX
round77 e round78 sotto).

Perché questa pipeline esiste: verificato che Open-Meteo, per il quorum
tropopausa, rappresenta malissimo proprio le fonti più autorevoli (ECMWF
arrivava con solo 5 livelli su 9 richiesti, a volte con disponibilità
incoerente nel tempo sullo stesso identico modello/livello/orario — vedi
discussione). Qui si va alle fonti primarie (bucket ufficiali via Herbie),
non attraverso un aggregatore terzo.

FIX (27/08/2026, operatore+assistente) — verificato via Termux/curl diretto
sull'indice .idx (non solo teoria): il prodotto GFS "pgrb2.0p25" (campi
comuni) NON include i livelli 225/175/125hPa — stesso schema aeronautico
standard di ECMWF, stesso salto 200↔150hPa che stiamo cercando di evitare.
Quei tre livelli SONO però disponibili nel prodotto separato "pgrb2b.0p25"
(campi non comuni), confermato con TMP e HGT presenti a 125/175/225mb.
GFS ora interroga ENTRAMBI i prodotti e unisce i livelli — non si assume
che pgrb2b contenga anche i livelli standard di pgrb2, quindi nessuno dei
due sostituisce l'altro, si sommano.

ECMWF Open Data resta capato a un massimo di 13-14 livelli isobarici
ufficiali (1000/925/850/700/600/500/400/300/250/200/150/100/50hPa) — 200 e
150hPa restano adiacenti, nessun livello intermedio disponibile da questa
fonte in nessun modo verificato finora. Tenuto comunque nel repo come
secondo controllo indipendente (riferimento istituzionale).

Usa Herbie per il download parziale via file indice (.idx): scarica solo i
messaggi GRIB richiesti (TMP/HGT o t/gh sui livelli), non il file intero.

NOTA (27/08/2026, operatore+assistente) — risolto un problema di datetime
tz-aware/naive (Herbie confronta internamente datetime naive) e verificato
che Herbie NON arrotonda da solo un orario arbitrario al ciclo sinottico
più vicino: bisogna passargli un orario di ciclo (00/06/12/18Z) valido
(pattern raccomandato dagli sviluppatori Herbie stessi, discussione
GitHub blaylockbk/Herbie#272). Inoltre un ciclo "in orario" non è detto
sia già pubblicato: GFS pubblica l'analisi f000 mediamente ~3h20-30min
dopo l'ora di riferimento, ECMWF Open Data tipicamente 6-8h. Lo script
prova il ciclo atteso e, se non ancora pubblicato, risale a ritroso
(vedi _resolve_cycle) invece di fallire secco.

FIX (28/08/2026, operatore+assistente) — priority esplicita per fonte
(GFS→aws, ECMWF→google): senza vincolo, Herbie a volte sceglieva
data.rda.ucar.edu (NCAR) per GFS, con certificato SSL auto-firmato non
valido lato server — fallimento silenzioso. GFS ed ECMWF via Herbie NON
condividono la stessa fonte affidabile (verificato dai log reali), quindi
niente valore unico di priority per entrambi.

FIX round77 (29/08/2026, operatore+assistente) — RISCRITTURA STRUTTURALE:
prima lo script scaricava UN SOLO istante (fxx=0, l'analisi) per lancio,
e il pannello lo trattava come valore costante per l'intera finestra di
previsione 0-12h — congelando un singolo scatto fotografico su 13 ore,
mentre una vera tropopausa si sposta nel tempo. Ora si scarica una SERIE:
- GFS: fxx 0,1,2,...,12 (orario — GFS pubblica dati orari fino a 120h,
  verificato nella documentazione NOMADS)
- ECMWF Open Data: fxx 0,3,6,9,12 (trioraria — VERIFICATO dal vivo via
  curl sull'indice .index il 29/08: fxx=1,2,4 restituiscono 404, solo
  0,3,6,9,12... esistono. Non è un limite di Herbie, è ECMWF stesso che
  non pubblica passi orari in questa finestra per il prodotto "oper").

FIX round78 (30/08/2026, operatore+assistente) — 0-12h NON bastava: la
finestra era ancorata a "quando gira il workflow" (JOB_HOUR_TO_CYCLE),
non a "quando verrà guardato il pannello". Tra due lanci cron possono
passare fino a 5h (es. 10→15 UTC), e il pannello può essere aperto in
qualsiasi momento di quell'intervallo — la sua finestra di previsione
parte da "adesso", non dall'orario del job. Caso reale osservato il
30/08: workflow alle 04:57 UTC (ciclo 00Z, serie 0-12h = 00:00-12:00
UTC), pannello lanciato alle 12:27 UTC (7.5h dopo) — l'intera serie era
già superata dal tempo, GRIB fetchato con successo ma MAI usato (zero
corrispondenze orarie). Finestra estesa a fxx 0-24 (GFS orario, ECMWF
ogni 3h) per coprire il caso peggiore: fino 6h di ritardo
ciclo→pubblicazione + fino 5h di deriva tra cron + 12h di previsione
del pannello.
Il ciclo sinottico (00/06/12/18Z) viene risolto UNA VOLTA per modello,
usando fxx=0 come test di pubblicazione (_resolve_cycle) — poi TUTTI gli
fxx della serie si riferiscono allo stesso identico ciclo, altrimenti
forecasts[] mescolerebbe run diversi e romperebbe la coerenza temporale
della serie (un fxx=6 da un ciclo e un fxx=9 da un altro non sono
confrontabili). Se un singolo fxx della serie manca per quel ciclo
specifico (raro, non lo stepback dell'intero modello — si accetta un
buco puntuale nella serie, sarà il pannello a colmarlo ora per ora con
l'altra fonte GRIB o col quorum Open-Meteo, non lo script a inseguire
un ciclo più vecchio solo per un'ora).

Fallisce in modo esplicito e loggato per ogni fxx che non produce livelli
validi — mai un JSON silenziosamente vuoto spacciato per successo. Se
un'INTERA fonte fallisce (zero fxx utilizzabili), l'altra viene comunque
pubblicata. L'intero job fallisce (exit 1) SOLO se ENTRAMBE le fonti sono
completamente vuote: in quel caso Belardo ricade sul quorum Open-Meteo a
4 modelli (fallback lato pannello, round72/77).
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from herbie import Herbie

CV_LAT = 41.035
CV_LON = 13.942
OUTPUT_PATH = "data/tropo-grib-latest.json"

JOB_HOUR_TO_CYCLE = {3: 0, 6: 0, 10: 6, 15: 12, 18: 12, 22: 18}

# FIX round77 — serie di forecast hour per modello. GFS orario (copre ogni
# ora 0-12 da solo); ECMWF trioraria (0,3,6,9,12 — il MASSIMO che la fonte
# offre in questa finestra, verificato dal vivo, non una scelta nostra).
# FIX round78 (30/08/2026, operatore+assistente) — 0-12h non bastava: il
# pannello può essere lanciato in qualunque momento tra un run cron e il
# successivo (gap fino a 5h, es. 10→15 UTC), e la sua finestra di
# previsione parte da "adesso", non da quando gira il workflow. Caso
# peggiore osservato dal vivo il 30/08: workflow alle 04:57 UTC (ciclo
# 00Z), pannello lanciato alle 12:27 UTC (7.5h dopo) — l'intera serie
# 0-12h era già superata dal tempo, gribTropoAt() non trovava mai
# corrispondenza nonostante il fetch fosse riuscito. Margine ricalcolato:
# fino 6h di ritardo ciclo→pubblicazione + fino 5h di deriva tra cron +
# 12h di finestra di previsione del pannello = serve coprire fino a fxx
# 24 dal ciclo, non 12.
GFS_FXX_SERIES = list(range(0, 25))
ECMWF_FXX_SERIES = [0, 3, 6, 9, 12, 15, 18, 21, 24]

PRIORITY_BY_MODEL = {'gfs': ['aws'], 'ifs': ['google'], 'ecmwf': ['google']}


def _resolve_cycle(model, product, target_cycle, max_stepback=2):
    """Trova il ciclo (00/06/12/18Z) più recente per cui fxx=0 è
    pubblicato, scendendo a ritroso di 6h alla volta se necessario.
    Chiamata UNA VOLTA per modello: tutta la serie oraria/trioraria di
    quel modello userà questo stesso ciclo, per coerenza (vedi nota in
    testa al file, FIX round77). Ritorna il datetime del ciclo risolto, o
    None se anche l'ultimo tentativo di stepback non ha fxx=0 pubblicato."""
    priority = PRIORITY_BY_MODEL.get(model)
    candidate = target_cycle
    for _ in range(max_stepback + 1):
        H = Herbie(candidate, model=model, product=product, fxx=0, priority=priority)
        if H.grib:
            return candidate
        candidate = candidate - timedelta(hours=6)
    return None


def _extract_one_fxx(model, product, search_string, cycle, fxx):
    """Interroga UN singolo fxx di UN prodotto e ritorna
    (temps_by_level, heights_by_level) — dict vuoti se nessun livello
    trovato o il file non esiste per questo fxx specifico. Non solleva
    mai eccezioni verso il chiamante."""
    priority = PRIORITY_BY_MODEL.get(model)
    H = Herbie(cycle, model=model, product=product, fxx=fxx, priority=priority)
    if not H.grib:
        return {}, {}
    ds = H.xarray(search_string, remove_grib=True)
    datasets = ds if isinstance(ds, list) else [ds]

    temps_by_level = {}
    heights_by_level = {}
    for d in datasets:
        point = d.sel(latitude=CV_LAT, longitude=CV_LON, method="nearest")
        if "isobaricInhPa" not in point.coords:
            continue
        levels = point["isobaricInhPa"].values
        if "t" in point.data_vars:
            for lvl, val in zip(levels, point["t"].values):
                temps_by_level[float(lvl)] = float(val) - 273.15  # K -> C
        if "gh" in point.data_vars:
            for lvl, val in zip(levels, point["gh"].values):
                heights_by_level[float(lvl)] = float(val)
    return temps_by_level, heights_by_level


def extract_model_series(model, products, search_string, fxx_series, job_now):
    """Ritorna (dict con runISO/forecasts[], lista_errori) — forecasts[]
    contiene una entry per ogni fxx della serie che ha prodotto almeno un
    livello utilizzabile; gli fxx falliti finiscono nella lista errori ma
    NON bloccano gli altri (successo parziale sulla serie, stessa
    filosofia "mai un buco silenzioso" già usata per i prodotti GFS
    multipli). Ritorna (None, [errore]) solo se l'intera serie è vuota."""
    errors = []
    target_cycle = JOB_HOUR_TO_CYCLE.get(job_now.hour, (job_now.hour // 6) * 6)
    target_cycle_dt = job_now.replace(hour=target_cycle, minute=0, second=0, microsecond=0)

    cycle = None
    for product in products:
        cycle = _resolve_cycle(model, product, target_cycle_dt)
        if cycle:
            break
    if not cycle:
        return None, [f"{model}: nessun ciclo pubblicato trovato (provato {target_cycle_dt.isoformat()} e stepback)"]

    forecasts = []
    for fxx in fxx_series:
        temps_by_level = {}
        heights_by_level = {}
        fxx_errors = []
        for product in products:
            try:
                t, h = _extract_one_fxx(model, product, search_string, cycle, fxx)
                temps_by_level.update(t)
                heights_by_level.update(h)
            except Exception as e:
                fxx_errors.append(f"{product}: {type(e).__name__}: {e}")

        common_levels = sorted(
            set(temps_by_level) & set(heights_by_level), reverse=True
        )  # discendente: dalla superficie verso l'alto, stessa
           # convenzione già usata lato Belardo (scanLapseTropopause)

        if not common_levels:
            detail = "; ".join(fxx_errors) if fxx_errors else "nessun livello trovato per questo fxx (probabile file non pubblicato)"
            errors.append(f"{model} fxx={fxx}: {detail}")
            continue

        valid_iso = (cycle + timedelta(hours=fxx)).replace(tzinfo=timezone.utc).isoformat()
        forecasts.append({
            "fxx": fxx,
            "validISO": valid_iso,
            "levels": [
                {"hPa": lvl, "tempC": round(temps_by_level[lvl], 2), "z": round(heights_by_level[lvl], 1)}
                for lvl in common_levels
            ],
        })

    if not forecasts:
        return None, errors or [f"{model}: nessun fxx della serie ha prodotto livelli utilizzabili"]

    return {"runISO": cycle.replace(tzinfo=timezone.utc).isoformat(), "forecasts": forecasts}, errors


def main():
    # naive UTC: Herbie confronta internamente datetime naive, tz-aware
    # farebbe fallire con TypeError (vedi nota in testa al file).
    job_now = datetime.now(timezone.utc).replace(tzinfo=None)

    result = {
        "generatedISO": datetime.now(timezone.utc).isoformat(),
        "gfs": None,
        "ecmwf": None,
    }
    all_errors = []

    print(f"=== GFS (pgrb2.0p25 + pgrb2b.0p25, fxx={GFS_FXX_SERIES[0]}-{GFS_FXX_SERIES[-1]} orario) ===")
    gfs_data, gfs_errors = extract_model_series(
        model="gfs",
        products=["pgrb2.0p25", "pgrb2b.0p25"],
        search_string=r":(TMP|HGT):\d+ mb:",
        fxx_series=GFS_FXX_SERIES,
        job_now=job_now,
    )
    result["gfs"] = gfs_data
    if gfs_data:
        n_ok = len(gfs_data["forecasts"])
        print(f"GFS OK — {n_ok}/{len(GFS_FXX_SERIES)} ore della serie, run {gfs_data['runISO']}")
    else:
        print(f"[ERRORE GFS] intera serie fallita", file=sys.stderr)
    if gfs_errors:
        all_errors.extend(f"GFS: {e}" for e in gfs_errors)
        for e in gfs_errors:
            print(f"  [buco GFS] {e}", file=sys.stderr)

    print(f"=== ECMWF Open Data (oper, fxx={ECMWF_FXX_SERIES}) ===")
    ecmwf_data, ecmwf_errors = extract_model_series(
        model="ecmwf",
        products=["oper"],
        search_string=r":(t|gh):\d+:",
        fxx_series=ECMWF_FXX_SERIES,
        job_now=job_now,
    )
    result["ecmwf"] = ecmwf_data
    if ecmwf_data:
        n_ok = len(ecmwf_data["forecasts"])
        print(f"ECMWF OK — {n_ok}/{len(ECMWF_FXX_SERIES)} ore della serie, run {ecmwf_data['runISO']}"
              f" (capato a max 13-14 livelli per limite della fonte, non un bug)")
    else:
        print(f"[ERRORE ECMWF] intera serie fallita", file=sys.stderr)
    if ecmwf_errors:
        all_errors.extend(f"ECMWF: {e}" for e in ecmwf_errors)
        for e in ecmwf_errors:
            print(f"  [buco ECMWF] {e}", file=sys.stderr)

    if all_errors:
        result["errors"] = all_errors

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\nScritto {OUTPUT_PATH}")

    # Fallisce l'intero job SOLO se ENTRAMBE le fonti sono completamente
    # vuote — una sola fonte, anche con buchi parziali nella serie, resta
    # un output utile e pubblicabile (il pannello colma i buchi ora per
    # ora con l'altra fonte GRIB o col quorum Open-Meteo, round77).
    if gfs_data is None and ecmwf_data is None:
        print(
            "\nEntrambe le fonti completamente fallite — nessun dato utile da "
            "pubblicare (Belardo ricadrà sul fallback quorum Open-Meteo a 4 modelli).",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
