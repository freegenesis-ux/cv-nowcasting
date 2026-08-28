#!/usr/bin/env python3
"""
extract_tropo_profile.py — pipeline GRIB2 (25-27/08/2026, operatore+assistente)

Estrae il profilo verticale (temperatura + quota geopotenziale) su TUTTI i
livelli di pressione isobarici disponibili in GFS e ECMWF Open Data, al
punto griglia più vicino a Castel Volturno (41.035N, 13.942E), dall'ANALISI
più recente (fxx=0) di ciascun modello — non una previsione, il punto di
massima fedeltà osservativa disponibile da ciascuna fonte.

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

ECMWF Open Data resta capato a un massimo di 13 livelli isobarici
ufficiali (1000/925/850/700/600/500/400/300/250/200/150/100/50hPa,
verificato sulla documentazione ECMWF + una issue GitHub che segnala anche
solo 9 popolati in pratica per t/u/v/r) — 200 e 150hPa restano adiacenti,
NESSUN livello intermedio disponibile da questa fonte in nessun modo
verificato finora. Tenuto comunque nel repo come secondo controllo
indipendente (riferimento istituzionale), ma non ci si aspetti che risolva
il salto 200-150hPa: solo GFS (pgrb2+pgrb2b) lo fa.

Usa Herbie per il download parziale via file indice (.idx): scarica solo i
messaggi GRIB richiesti (TMP/HGT o t/gh sui livelli), non il file intero.

Fallisce in modo esplicito e loggato per ogni fonte che non produce
livelli validi — mai un JSON silenziosamente vuoto spacciato per successo.
Se UNA fonte fallisce, l'altra viene comunque pubblicata (degradazione
dichiarata, non un blocco totale). L'intero job fallisce (exit 1) SOLO se
ENTRAMBE le fonti falliscono: in quel caso non c'è nulla di utile da
pubblicare, e Belardo ricade sul quorum Open-Meteo a 4 modelli (fallback
già deciso lato pannello, round72).

NOTA ONESTA (lasciata a scopo di manutenzione futura): le search_string
sono scritte secondo le convenzioni documentate di Herbie/wgrib2, ma non
sono state verificate contro un'esecuzione live completa di Herbie stesso
al momento della scrittura (solo l'indice .idx grezzo via curl è stato
controllato manualmente). È previsto che il primo run reale dell'Action
possa richiedere un aggiustamento, specialmente per ECMWF Open Data, la
cui convenzione di naming nei file .idx è meno documentata pubblicamente.
Controllare i log del job "Estrai profilo tropopausa" ad ogni run finché
non si stabilizza.

NOTA (27/08/2026, operatore+assistente) — risolto un problema di datetime
tz-aware/naive (Herbie confronta internamente datetime naive) e verificato
che Herbie NON arrotonda da solo un orario arbitrario al ciclo sinottico
più vicino: bisogna passargli un orario di ciclo (00/06/12/18Z) valido
(pattern raccomandato dagli sviluppatori Herbie stessi, discussione
GitHub blaylockbk/Herbie#272). Inoltre un ciclo "in orario" non è detto
sia già pubblicato: GFS pubblica l'analisi f000 mediamente ~3h20-30min
dopo l'ora di riferimento, ECMWF Open Data tipicamente 6-8h (dati
osservati, non la sonda che "ci mette" quel tempo ad influenzare il
modello — l'assimilazione chiude entro poche ore dall'orario di
riferimento, il ritardo è calcolo/QC/distribuzione a valle). Lo script
ora prova il ciclo atteso e, se non ancora pubblicato, risale a ritroso
(vedi _find_published_run) invece di fallire secco.
"""
import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone

from herbie import Herbie

CV_LAT = 41.035
CV_LON = 13.942
OUTPUT_PATH = "data/tropo-grib-latest.json"

# Mappa ora UTC di esecuzione del job (cron: 3,6,10,15,18,22) -> ciclo
# sinottico del modello (00/06/12/18Z) che quel run VORREBBE interrogare
# (punto di partenza per lo step-back qui sotto, non una garanzia che sia
# già pubblicato). 03 e 15 puntano a 00Z/12Z per l'allineamento coi
# sondaggi Wyoming (vedi commento in tropo-grib-pipeline.yml); gli altri
# quattro guardano al ciclo precedente con margine.
JOB_HOUR_TO_CYCLE = {3: 0, 6: 0, 10: 6, 15: 12, 18: 12, 22: 18}


def _target_cycle(now):
    """Arrotonda `now` (naive UTC) al ciclo sinottico 00/06/12/18Z di
    partenza. Herbie non arrotonda da solo: si aspetta in input l'orario
    esatto del ciclo, non un orario arbitrario nella finestra (pattern
    ufficiale Herbie: floor su 6h prima di passare la data — vedi
    discussione GitHub blaylockbk/Herbie#272). Il fallback
    `(now.hour // 6) * 6` copre run manuali fuori dagli orari previsti
    (es. workflow_dispatch)."""
    cycle_hour = JOB_HOUR_TO_CYCLE.get(now.hour, (now.hour // 6) * 6)
    return now.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)


def _find_published_run(model, product, target_cycle, max_stepback=2):
    """Prova `target_cycle`, poi risale a ritroso di un ciclo (-6h) alla
    volta fino a `max_stepback` tentativi in più, finché non trova un run
    i cui file sono DAVVERO pubblicati (H.grib valorizzato) — non basta
    che l'orario sia "nel passato", il file deve esistere sul bucket.

    Perché serve: la pubblicazione di un ciclo non è istantanea rispetto
    all'orario di riferimento. GFS pubblica l'analisi (f000) di un ciclo
    mediamente ~3h20-30min dopo l'ora di riferimento (dato osservato su
    NOMADS in un caso misurato, non "si presume disponibile subito");
    ECMWF Open Data è tipicamente più lento (comunemente citato 6-8h).
    Questo NON è il tempo che i dati da radiosonda impiegano a entrare
    nel modello — l'assimilazione usa una finestra di poche ore intorno
    all'orario di riferimento e la sonda è già dentro quella finestra —
    è tempo di calcolo/QC/distribuzione A VALLE dell'assimilazione, un
    passaggio separato. Per questo il ciclo "giusto" secondo
    JOB_HOUR_TO_CYCLE potrebbe non essere ancora pubblicato quando il
    job gira, specialmente per ECMWF sugli slot più stretti (03/15h).

    FIX (28/08/2026, operatore+assistente) — priority esplicita per fonte
    invece di lasciare Herbie scegliere da solo: trovato dal vivo che,
    senza vincolo, Herbie a volte sceglie data.rda.ucar.edu (NCAR
    Research Data Archive) per GFS invece del bucket AWS (verificato
    funzionante a mano via curl il 27/08). NCAR RDA ha un certificato
    SSL auto-firmato non valido lato server — causa SSLCertVerificationError,
    fallimento SILENZIOSO di quella fonte per quel lancio (GFS: null nel
    JSON pubblicato, "gfs" scomparso senza errore visibile a schermo —
    solo nel campo "errors" del JSON). Non è un problema risolvibile da
    parte nostra (è il certificato del server NCAR); la soluzione è
    restringere la ricerca alla fonte nota affidabile per QUEL modello.
    ATTENZIONE: GFS ed ECMWF via Herbie NON condividono la stessa fonte
    affidabile — verificato dal log del primo run riuscito (27/08): GFS
    via AWS, ECMWF via Google Cloud Storage. Una priority fissa uguale
    per entrambi avrebbe rotto uno dei due modelli per "risolvere"
    l'altro — da qui la mappa esplicita sotto invece di un valore unico."""
    priority_by_model = {'gfs': ['aws'], 'ifs': ['google'], 'ecmwf': ['google']}
    priority = priority_by_model.get(model)  # None se modello non mappato: nessun vincolo, comportamento originale
    candidate = target_cycle
    H = None
    for _ in range(max_stepback + 1):
        H = Herbie(candidate, model=model, product=product, fxx=0, priority=priority)
        # DIAGNOSTICA TEMPORANEA (28/08/2026, operatore+assistente) — 'rda'
        # (NCAR) non compare nella documentazione ufficiale Herbie come
        # fonte valida per 'priority' (solo aws/nomads/google/azure/pando/
        # pando2), eppure priority=['aws'] non ha impedito il fallimento su
        # data.rda.ucar.edu. Serve vedere quali fonti Herbie considera
        # DAVVERO per questo modello/prodotto, per capire se RDA è
        # governata da priority o è un fallback fuori dal suo controllo.
        # Da rimuovere una volta chiarito.
        print(f"[DIAG {model}/{product}] priority richiesta={priority} | "
              f"H.priority={getattr(H,'priority',None)} | "
              f"H.SOURCES={list(getattr(H,'SOURCES',{}).keys()) if hasattr(H,'SOURCES') else 'n/d'} | "
              f"grib_source={getattr(H,'grib_source',None)}")
        if H.grib:
            return H
        candidate = candidate - timedelta(hours=6)
    return H  # ultimo tentativo: se ancora senza grib, H.xarray() sotto
              # solleverà e il chiamante lo registrerà come errore


def _extract_one_product(model, product, search_string, target_cycle):
    """Interroga UN prodotto/fonte e ritorna (temps_by_level, heights_by_level,
    run_iso) — dict vuoti se nessun livello trovato. Non solleva mai
    eccezioni verso il chiamante oltre quelle già gestite dentro."""
    H = _find_published_run(model, product, target_cycle)
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

    run_iso = H.date.replace(tzinfo=timezone.utc).isoformat()
    return temps_by_level, heights_by_level, run_iso


def extract_model_profile(model, products, search_string):
    """Ritorna (dict con runISO/levels, None) in caso di successo,
    (None, messaggio errore) in caso di fallimento — mai un'eccezione
    che risale fuori: il chiamante deve poter continuare con l'altra
    fonte anche se questa fallisce.

    `products` è una lista: i livelli trovati in ciascun prodotto vengono
    UNITI (non sostituiti) — pensato per GFS, dove pgrb2.0p25 (livelli
    standard) e pgrb2b.0p25 (125/175/225hPa) si completano a vicenda.
    Per fonti con un solo prodotto (es. ECMWF) passare una lista di un
    elemento: il comportamento è identico allo script precedente."""
    try:
        # naive UTC e arrotondato al ciclo sinottico corretto (_target_cycle):
        # tz-aware farebbe fallire il confronto interno di Herbie con
        # TypeError ("can't compare offset-naive and offset-aware
        # datetimes"); un orario non arrotondato (es. le 03:00 esatte di
        # esecuzione del job) farebbe cercare un ciclo 03Z che non esiste.
        # H.date resta naive-UTC, coerente con l'uso a riga ~100
        # (H.date.replace(tzinfo=timezone.utc)).
        now = _target_cycle(datetime.now(timezone.utc).replace(tzinfo=None))
        temps_by_level = {}
        heights_by_level = {}
        run_iso = None
        product_errors = []

        for product in products:
            try:
                t, h, r = _extract_one_product(model, product, search_string, now)
                temps_by_level.update(t)
                heights_by_level.update(h)
                run_iso = run_iso or r  # primo run trovato, dovrebbero coincidere
            except Exception as e:
                # un prodotto che fallisce non deve bloccare gli altri
                # (es. pgrb2b non ancora pubblicato mentre pgrb2 sì)
                product_errors.append(f"{product}: {type(e).__name__}: {e}")

        common_levels = sorted(
            set(temps_by_level) & set(heights_by_level), reverse=True
        )  # discendente: dalla superficie verso l'alto, stessa
           # convenzione già usata lato Belardo (scanLapseTropopause)

        if not common_levels:
            detail = "; ".join(product_errors) if product_errors else (
                "nessun livello con sia temperatura che quota disponibili — "
                "possibile search_string da correggere, vedi nota in testa al file"
            )
            return None, detail

        levels_out = [
            {
                "hPa": lvl,
                "tempC": round(temps_by_level[lvl], 2),
                "z": round(heights_by_level[lvl], 1),
            }
            for lvl in common_levels
        ]
        result = {"runISO": run_iso, "levels": levels_out}
        if product_errors:
            # successo parziale: alcuni prodotti sono falliti ma almeno
            # uno ha prodotto dati utilizzabili — lo segnaliamo comunque,
            # non lo nascondiamo (stessa filosofia "mai un JSON silenzioso")
            result["partialErrors"] = product_errors
        return result, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"


def main():
    result = {
        "generatedISO": datetime.now(timezone.utc).isoformat(),
        "gfs": None,
        "ecmwf": None,
    }
    errors = []

    print("=== GFS (pgrb2.0p25 + pgrb2b.0p25, fxx=0) ===")
    gfs_data, gfs_err = extract_model_profile(
        model="gfs",
        products=["pgrb2.0p25", "pgrb2b.0p25"],
        search_string=r":(TMP|HGT):\d+ mb:",
    )
    result["gfs"] = gfs_data
    if gfs_err:
        errors.append(f"GFS: {gfs_err}")
        print(f"[ERRORE GFS] {gfs_err}", file=sys.stderr)
    else:
        n = len(gfs_data['levels'])
        print(f"GFS OK — {n} livelli, run {gfs_data['runISO']}")
        if "partialErrors" in gfs_data:
            print(f"  (parziale: {gfs_data['partialErrors']})", file=sys.stderr)

    print("=== ECMWF Open Data (oper, fxx=0) ===")
    ecmwf_data, ecmwf_err = extract_model_profile(
        model="ecmwf",
        products=["oper"],
        search_string=r":(t|gh):\d+:",
    )
    result["ecmwf"] = ecmwf_data
    if ecmwf_err:
        errors.append(f"ECMWF: {ecmwf_err}")
        print(f"[ERRORE ECMWF] {ecmwf_err}", file=sys.stderr)
    else:
        print(f"ECMWF OK — {len(ecmwf_data['levels'])} livelli, run {ecmwf_data['runISO']}"
              f" (capato a max 13 livelli per limite della fonte, non un bug)")

    if errors:
        result["errors"] = errors

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\nScritto {OUTPUT_PATH}")

    # Fallisce l'intero job SOLO se ENTRAMBE le fonti sono fallite — una
    # sola fonte disponibile è comunque un output utile e pubblicabile.
    if gfs_data is None and ecmwf_data is None:
        print(
            "\nEntrambe le fonti fallite — nessun dato utile da pubblicare "
            "(Belardo ricadrà sul fallback quorum Open-Meteo a 4 modelli).",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
