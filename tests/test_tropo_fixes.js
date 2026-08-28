#!/usr/bin/env node
// Test di regressione per i 3 fix tropopausa (28/08/2026).
// Uso: node test_tropo_fixes.js [percorso/a/index.html]
//
// Non duplica la logica a mano: estrae le funzioni VERE da index.html
// (scanLapseTropopause, weightedMedian, selectGribSourcesByAge) via
// regex ed eval, così il test fallisce se qualcuno le modifica senza
// aggiornare anche il comportamento atteso qui sotto.

const fs = require('fs');
const path = process.argv[2] || 'index.html';
const html = fs.readFileSync(path, 'utf8');

function extractFn(name) {
  // Cattura dalla dichiarazione "function NAME(" fino alla '}' che
  // chiude la graffa di apertura della funzione (bracket counting,
  // così funziona anche con funzioni lunghe/annidate).
  const startMatch = html.match(new RegExp(`function ${name}\\s*\\([^)]*\\)\\s*\\{`));
  if (!startMatch) throw new Error(`Funzione ${name} non trovata in ${path}`);
  const start = startMatch.index;
  let depth = 0, i = start;
  for (; i < html.length; i++) {
    if (html[i] === '{') depth++;
    else if (html[i] === '}') { depth--; if (depth === 0) { i++; break; } }
  }
  return html.slice(start, i);
}

const src = [extractFn('scanLapseTropopause'), extractFn('weightedMedian'), extractFn('selectGribSourcesByAge')].join('\n');
eval(src); // definisce scanLapseTropopause, weightedMedian, selectGribSourcesByAge nello scope locale

let pass = 0, fail = 0;
function check(name, actual, expected, tolerance = 0) {
  const ok = typeof expected === 'number'
    ? Math.abs(actual - expected) <= tolerance
    : JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) { pass++; console.log(`✓ ${name}`); }
  else { fail++; console.log(`✗ ${name}\n    atteso:  ${JSON.stringify(expected)}\n    ottenuto: ${JSON.stringify(actual)}`); }
}

console.log('── PATCH 1: floor minHPa in scanLapseTropopause ──');

// Caso reale che ha causato il bug originale (JSON pubblicato 28/08 02:06Z,
// inversione notturna da suolo: 1000hPa più caldo di quota crescente fino
// a 925hPa prima di riprendere il lapse normale).
const ecmwfRealCase = [
  { hPa: 1000.0, t: 28.99, z: 157.2 }, { hPa: 925.0, t: 30.65, z: 854.8 },
  { hPa: 850.0, t: 25.32, z: 1603.2 }, { hPa: 700.0, t: 12.42, z: 3269.8 },
  { hPa: 600.0, t: 1.54, z: 4538.1 }, { hPa: 500.0, t: -7.95, z: 5981.0 },
  { hPa: 400.0, t: -19.22, z: 7677.5 }, { hPa: 300.0, t: -35.76, z: 9747.1 },
  { hPa: 250.0, t: -44.88, z: 10988.2 }, { hPa: 200.0, t: -54.8, z: 12446.4 },
  { hPa: 150.0, t: -63.17, z: 14241.9 }, { hPa: 100.0, t: -62.53, z: 16729.1 },
  { hPa: 50.0, t: -57.12, z: 21040.4 }, { hPa: 10.0, t: -41.65, z: 31555.2 },
];
check('senza floor, il bug rimane riproducibile (regressione nota)',
  scanLapseTropopause(ecmwfRealCase).z, 157, 0);
check('con floor 500hPa, ignora l\'inversione notturna e trova la vera tropopausa',
  scanLapseTropopause(ecmwfRealCase, 500).z, 14503, 50);

// Profilo pulito (nessun livello sotto 300hPa): il floor non deve
// cambiare nulla rispetto al comportamento storico.
const cleanProfile = [
  { hPa: 300, t: -34.5, z: 9739 }, { hPa: 250, t: -43.5, z: 10989 },
  { hPa: 200, t: -54.0, z: 12453 }, { hPa: 150, t: -63.0, z: 14260 },
  { hPa: 100, t: -67.0, z: 16708 }, { hPa: 70, t: -60.0, z: 18896 },
];
const withoutFloor = scanLapseTropopause(cleanProfile);
const withFloor = scanLapseTropopause(cleanProfile, 500);
check('su un profilo già pulito, il floor non altera il risultato (no regressioni sui path Open-Meteo)',
  withFloor.z, withoutFloor.z, 0);

console.log('\n── PATCH 3: selectGribSourcesByAge (quorum a due solo se stessa età) ──');

const now = Date.parse('2026-08-28T05:00:00Z');
const gfsFresh = { model: 'gfs', runMs: now };
const ecmwfFresh = { model: 'ecmwf', runMs: now - 0.5 * 3600000 }; // 30min prima
const ecmwfStale = { model: 'ecmwf', runMs: now - 7 * 3600000 };   // 7h prima (tipico ECMWF Open Data)

check('run entro 1h → nessuno scartato, quorum a due attivo',
  selectGribSourcesByAge([gfsFresh, ecmwfFresh], 1).discarded, null);
check('run entro 1h → usable contiene entrambi',
  selectGribSourcesByAge([gfsFresh, ecmwfFresh], 1).usable.length, 2);

const staleCase = selectGribSourcesByAge([gfsFresh, ecmwfStale], 1);
check('run distanti 7h (>1h) → scarta il più vecchio (ecmwf)',
  staleCase.discarded, 'ecmwf');
check('run distanti 7h → resta solo il più recente (gfs)',
  staleCase.usable.map(u => u.model), ['gfs']);

check('una sola fonte disponibile → nessun confronto, passa invariata',
  selectGribSourcesByAge([gfsFresh], 1).usable.map(u => u.model), ['gfs']);
check('zero fonti disponibili → nessun confronto, passa invariata',
  selectGribSourcesByAge([], 1).usable, []);

// Caso limite: esattamente alla soglia (1.0h) non deve scartare (<=), solo oltre.
const atThreshold = selectGribSourcesByAge(
  [{ model: 'a', runMs: now }, { model: 'b', runMs: now - 1 * 3600000 }], 1
);
check('scarto esattamente = soglia (1.0h) → NON scarta (confine incluso)',
  atThreshold.discarded, null);

console.log(`\n${pass} passati, ${fail} falliti su ${pass + fail} test.`);
process.exit(fail > 0 ? 1 : 0);
