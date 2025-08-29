/** =========================================================================
 * Gemini for Sheets — robust, grounded, structured.
 * Features:
 * - =GEMINI() text answers with optional Search grounding
 * - =GEMINI_RICH() inline clickable citations (RichTextValue)
 * - =GEMINI_SOURCES() spill [title, url] rows
 * - =GEMINI_JSON() raw JSON candidate dump
 * - =GEMINI_BATCH_JSON() one-call-per-row JSON (cost saver)
 * - =GEMINI_BATCH_TABLE() one-call-per-row spill array, by keys
 * - =JSONPATH(json, "$.a[0].b") to pick values into columns
 * Menus:
 * - Set API key & defaults
 * - Batch wizard: run once now or schedule monthly
 * ========================================================================= */
//test
/** =========================================================================
 *  Gemini for Sheets — robust, grounded, structured.
 * ========================================================================= */

const DEFAULT_MODEL = 'gemini-2.5-flash';
const GEMINI_HOST   = 'https://generativelanguage.googleapis.com';
const GEMINI_PATH   = '/v1beta/models/%MODEL%:generateContent'; // official REST

const PROP_API_KEY        = 'GEMINI_API_KEY';
const PROP_DEFAULTS       = 'GEMINI_DEFAULTS_JSON';
const PROP_BATCH          = 'GEMINI_BATCH_CONFIG_JSON';
const PROP_TRIGGER_META   = 'GEMINI_TRIGGER_META_JSON';
const PROP_CACHE_SALT     = 'GEMINI_CACHE_SALT';

const CACHE_TTL_SEC  = 6 * 60 * 60; // 6 hours for identical prompts
const MAX_CELL_CHARS = 50000;       // safety to avoid overlong cell content
const SOURCE_LIMIT   = 20;

function ping_() {
  return 'pong';
}

/* ------------------------------- MENU ---------------------------------- */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Gemini for Sheets')
    .addItem('Set API key & defaults', 'showApiKeySidebar_')
    .addSeparator()
    .addItem('Batch wizard (one-call-per-row)', 'showBatchDialog_')
    .addItem('Run batch now', 'runBatchNow_')
    .addItem('Create/Update monthly trigger', 'ensureMonthlyTrigger_')
    .addSeparator()
    .addItem('Clear cache', 'clearGeminiCache_')
    .addToUi();
}
//* ------------------------------- CONFIG -------------------------------- */

function showApiKeySidebar_() {
  const html = HtmlService.createHtmlOutputFromFile('Sidebar').setTitle('Gemini: API key & defaults');
  SpreadsheetApp.getUi().showSidebar(html);
}

function saveGeminiApiKey_(key) {
  key = (key || '').trim();
  if (!key) throw new Error('Empty API key.');
  PropertiesService.getScriptProperties().setProperty(PROP_API_KEY, key);
  return 'Saved';
}

function getGeminiApiKey_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_API_KEY);
}

function saveDefaults_(defaultsJson) {
  // defaultsJson is a stringified object from the sidebar
  PropertiesService.getUserProperties().setProperty(PROP_DEFAULTS, defaultsJson || '{}');
  return 'Saved';
}

function getDefaults_() {
  const raw = PropertiesService.getUserProperties().getProperty(PROP_DEFAULTS);
  let j = {};
  try { j = raw ? JSON.parse(raw) : {}; } catch (_) {}
  // Normalize
  return {
    model: (j.model || DEFAULT_MODEL),
    temperature: (typeof j.temperature === 'number') ? j.temperature : 0.2,
    maxOutputTokens: (typeof j.maxOutputTokens === 'number') ? j.maxOutputTokens : 1024,
    search: (j.search === undefined) ? true : !!j.search
  };
}
function ping_() { return 'pong'; }


/* --------------------------- CORE HTTP CALL ---------------------------- */
function callGemini_(body, model, apiKey) {
  const url = (GEMINI_HOST + GEMINI_PATH).replace('%MODEL%', encodeURIComponent(model || DEFAULT_MODEL));
  const payload = JSON.stringify(body);
  const opts = {
    method: 'post',
    contentType: 'application/json',
    payload,
    headers: { 'x-goog-api-key': apiKey },
    muteHttpExceptions: true
  };
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const resp = UrlFetchApp.fetch(url, opts);
    const code = resp.getResponseCode();
    const text = resp.getContentText();
    if (code >= 200 && code < 300) {
      try { return JSON.parse(text); } catch (e) { throw new Error('Invalid JSON from Gemini.'); }
    }
    if (code === 429 || (code >= 500 && code <= 599)) {
      Utilities.sleep(300 * attempt); // backoff with jitter-like spacing
      continue;
    }
    // Non-retryable
    try {
      const j = JSON.parse(text);
      if (j && j.error && j.error.message) throw new Error(`Gemini API error (${code}): ${j.error.message}`);
    } catch (_) {}
    throw new Error(`Gemini API error (${code}).`);
  }
  throw new Error('Gemini API transient error. Try again.');
}

/* ----------------------------- UTILITIES ------------------------------- */
function flattenToString_(val) {
  if (val == null) return '';
  if (Array.isArray(val)) return val.map(r => r.map(v => v == null ? '' : String(v)).join(' ')).join('\n');
  return String(val);
}

function getCacheSalt_() {
  return PropertiesService.getUserProperties().getProperty(PROP_CACHE_SALT) || '';
}

function makeCacheKey_(model, body) {
  const salt = getCacheSalt_(); // rotate via Clear cache
  const raw = model + '|' + JSON.stringify(body) + '|' + salt;
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, raw);
  return 'GMN|' + Utilities.base64EncodeWebSafe(bytes);
}

function clearGeminiCache_() {
  PropertiesService.getUserProperties().setProperty(PROP_CACHE_SALT, String(Date.now()));
  SpreadsheetApp.getActive().toast('Cache invalidated for future calls.');
}

/* ------------ RESPONSE PROCESSING: TEXT, SOURCES, RICH LINKS ---------- */
function extractFirstCandidate_(json) {
  const cand = json && json.candidates && json.candidates[0];
  if (!cand) {
    const pf = json && json.promptFeedback;
    if (pf && pf.blockReason) return { text: `⚠️ Blocked: ${pf.blockReason}`, gm: null };
    return { text: '⚠️ No candidates returned.', gm: null };
  }
  let text = '';
  if (cand.content && Array.isArray(cand.content.parts)) {
    text = cand.content.parts.map(p => p.text || '').join('');
  } else if (cand.text) {
    text = cand.text;
  }
  const gm = cand.groundingMetadata || cand.grounding_metadata || null;
  return { text, gm };
}

function sourcesFromGM_(gm) {
  const out = [];
  if (!gm || !Array.isArray(gm.groundingChunks)) return out;
  const seen = {};
  for (const ch of gm.groundingChunks) {
    const web = ch && ch.web;
    const uri = web && web.uri;
    if (!uri) continue;
    if (seen[uri]) continue;
    seen[uri] = true;
    out.push([web.title || uri, uri]);
    if (out.length >= SOURCE_LIMIT) break;
  }
  return out;
}

function inlineCitationsRichText_(text, gm) {
  // Build an inline "[n]" for each groundingSupport segment, descending by endIndex
  if (!gm || !Array.isArray(gm.groundingSupports) || !Array.isArray(gm.groundingChunks)) {
    // no metadata — just return plain text RichText
    return SpreadsheetApp.newRichTextValue().setText(text).build();
  }
  const supports = gm.groundingSupports.slice().sort((a, b) => {
    const ea = (a.segment && a.segment.endIndex) || 0;
    const eb = (b.segment && b.segment.endIndex) || 0;
    return eb - ea;
  });
  let work = text;
  // Map chunk index -> [uri]
  const chunkUris = gm.groundingChunks.map(c => (c && c.web && c.web.uri) || null);

  for (const s of supports) {
    const end = s.segment && typeof s.segment.endIndex === 'number' ? s.segment.endIndex : null;
    const idxs = (s.groundingChunkIndices || []).filter(i => i >= 0 && i < chunkUris.length && !!chunkUris[i]);
    if (end == null || !idxs.length) continue;
    // Build " [1][2]" style
    const marks = idxs.map(i => `[${i + 1}]`).join('');
    work = work.slice(0, end) + ' ' + marks + work.slice(end);
  }
  // Now build RichText and add link styles for each occurrence of [n]
  const builder = SpreadsheetApp.newRichTextValue().setText(work);
  const re = /\[(\d+)\]/g;
  let m;
  while ((m = re.exec(work)) !== null) {
    const n = parseInt(m[1], 10);
    const uri = chunkUris[n - 1];
    if (uri) {
      const start = m.index;
      const end = start + m[0].length;
      builder.setLinkUrl(start, end, uri);
    }
  }
  return builder.build();
}

/* ------------------------------ BUILD BODY ---------------------------- */
function buildRequestBody_(prompt, opts) {
  const d = getDefaults_();
  const search = (opts.search === undefined) ? d.search : !!opts.search;
  const body = {
    contents: [{ parts: [{ text: prompt }]}],
    generationConfig: {
      temperature: (typeof opts.temperature === 'number') ? opts.temperature : d.temperature,
      maxOutputTokens: (typeof opts.maxOutputTokens === 'number') ? opts.maxOutputTokens : d.maxOutputTokens
    }
  };
  if (opts.responseMimeType) {
    body.generationConfig.responseMimeType = opts.responseMimeType; // "application/json"
  }
  if (opts.responseSchema) {
    body.generationConfig.responseSchema = opts.responseSchema;      // JSON schema object
  }
  if (search) {
    body.tools = [{ google_search: {} }]; // Ground with Google Search
  }
  return body;
}

/* ---------------------------- PUBLIC FUNCTIONS ------------------------ */
/** Basic: =GEMINI("prompt", A2, ...) */
function GEMINI() {
  if (arguments.length === 0) return '⚠️ Provide a prompt';
  const args = Array.prototype.slice.call(arguments).map(a => flattenToString_(a));
  let opts = {};
  if (args[0].trim().startsWith('{') && args[0].trim().endsWith('}')) {
    try { opts = JSON.parse(args.shift()); } catch (_) {}
  }
  const prompt = args.join(' ').trim();
  if (!prompt) return '⚠️ Empty prompt.';
  const apiKey = getGeminiApiKey_();
  if (!apiKey) return '⚠️ Set API key via menu.';
  const model = (opts.model && String(opts.model)) || getDefaults_().model;

  const body = buildRequestBody_(prompt, opts);
  const cacheKey = makeCacheKey_(model, body);
  const cache = CacheService.getUserCache();
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const json = callGemini_(body, model, apiKey);
  let { text, gm } = extractFirstCandidate_(json);
  // Append sources list if present
  const src = sourcesFromGM_(gm);
  if (src.length) {
    const lines = src.map((r, i) => `${i + 1}. ${r[0]} — ${r[1]}`);
    text = `${text}\n\nSources:\n${lines.join('\n')}`;
  }
  if (text.length > MAX_CELL_CHARS) text = text.slice(0, MAX_CELL_CHARS - 1) + '…';
  cache.put(cacheKey, text, CACHE_TTL_SEC);
  return text;
}

/** Rich: inline [n] links as RichTextValue. */
function GEMINI_RICH() {
  if (arguments.length === 0) return '⚠️ Provide a prompt';
  const args = Array.prototype.slice.call(arguments).map(a => flattenToString_(a));
  let opts = {};
  if (args[0].trim().startsWith('{') && args[0].trim().endsWith('}')) {
    try { opts = JSON.parse(args.shift()); } catch (_) {}
  }
  const prompt = args.join(' ').trim();
  if (!prompt) return '⚠️ Empty prompt.';
  const apiKey = getGeminiApiKey_();
  if (!apiKey) return '⚠️ Set API key via menu.';
  const model = (opts.model && String(opts.model)) || getDefaults_().model;

  const body = buildRequestBody_(prompt, opts);
  const cacheKey = makeCacheKey_(model, body) + '|RICH';
  const cache = CacheService.getUserCache();
  const cached = cache.get(cacheKey);
  if (cached) return SpreadsheetApp.newRichTextValue().setText(cached).build(); // link data not cached

  const json = callGemini_(body, model, apiKey);
  const { text, gm } = extractFirstCandidate_(json);
  // Build RichText with inline citations
  const rtv = inlineCitationsRichText_(text, gm);
  return rtv;
}

/** Sources: spill array of [title, url] */
function GEMINI_SOURCES() {
  if (arguments.length === 0) return [['⚠️ Provide a prompt']];
  const args = Array.prototype.slice.call(arguments).map(a => flattenToString_(a));
  let opts = {};
  if (args[0].trim().startsWith('{') && args[0].trim().endsWith('}')) {
    try { opts = JSON.parse(args.shift()); } catch (_) {}
  }
  const prompt = args.join(' ').trim();
  if (!prompt) return [['⚠️ Empty prompt']];
  const apiKey = getGeminiApiKey_();
  if (!apiKey) return [['⚠️ Set API key via menu']];
  const model = (opts.model && String(opts.model)) || getDefaults_().model;

  const body = buildRequestBody_(prompt, opts);
  const json = callGemini_(body, model, apiKey);
  const { gm } = extractFirstCandidate_(json);
  const rows = sourcesFromGM_(gm);
  return rows.length ? rows : [['(no sources)','']];
}

/** Raw JSON candidate (string). */
function GEMINI_JSON() {
  if (arguments.length === 0) return '⚠️ Provide a prompt';
  const args = Array.prototype.slice.call(arguments).map(a => flattenToString_(a));
  let opts = {};
  if (args[0].trim().startsWith('{') && args[0].trim().endsWith('}')) {
    try { opts = JSON.parse(args.shift()); } catch (_) {}
  }
  const prompt = args.join(' ').trim();
  if (!prompt) return '⚠️ Empty prompt.';
  const apiKey = getGeminiApiKey_();
  if (!apiKey) return '⚠️ Set API key via menu.';
  const model = (opts.model && String(opts.model)) || getDefaults_().model;

  const body = buildRequestBody_(prompt, opts);
  const json = callGemini_(body, model, apiKey);
  // Return a condensed string
  return JSON.stringify(json && json.candidates && json.candidates[0] || json || {}, null, 2);
}

/* ------------------------------ BATCH MODE ----------------------------- */
/**
 * =GEMINI_BATCH_JSON(optionsJson, "prompt", A2:E2)
 * optionsJson supports:
 * { schema?: {..}, keys?: ["summary","founder",...], search?: true, temperature?: 0.2, maxOutputTokens?: 1200, model?: "gemini-2.5-flash" }
 * If schema is provided, we enforce structured JSON.
 * Prompt tip: include instructions; we’ll append "Respond ONLY with valid JSON."
 */
function GEMINI_BATCH_JSON(optionsJson, prompt) {
  const a = Array.prototype.slice.call(arguments);
  if (a.length < 2) return '⚠️ Usage: =GEMINI_BATCH_JSON(optionsJson, "prompt", [cells...])';
  let opts = {};
  try { opts = JSON.parse(String(optionsJson)); } catch (_) { return '⚠️ optionsJson must be JSON'; }
  const ranges = a.slice(2).map(flattenToString_).join('\n').trim();
  const apiKey = getGeminiApiKey_();
  if (!apiKey) return '⚠️ Set API key via menu.';
  const model = (opts.model && String(opts.model)) || getDefaults_().model;

  let fullPrompt = `${prompt}\n\nContext:\n${ranges}\n\nReturn ONLY JSON.`;
  const reqOpts = Object.assign({}, opts);
  if (opts.schema) {
    reqOpts.responseMimeType = 'application/json';
    reqOpts.responseSchema = opts.schema; // must be a JSON schema object
  } else {
    reqOpts.responseMimeType = 'application/json';
  }

  const body = buildRequestBody_(fullPrompt, reqOpts);
  const cacheKey = makeCacheKey_(model, body);
  const cache = CacheService.getUserCache();
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const json = callGemini_(body, model, apiKey);
  // When responseMimeType=application/json, model's primary text is JSON
  const cand = json && json.candidates && json.candidates[0];
  let out = '';
  if (cand && cand.content && cand.content.parts && cand.content.parts[0] && cand.content.parts[0].text) {
    out = cand.content.parts[0].text.trim();
  } else {
    // fallback: try any text field
    const { text } = extractFirstCandidate_(json);
    out = (text || '').trim();
  }
  if (!out) out = '{}';
  cache.put(cacheKey, out, CACHE_TTL_SEC);
  return out;
}

/**
 * =GEMINI_BATCH_TABLE(optionsJson, "prompt", A2:E2)
 * Returns a single-row spill array in the key order.
 * optionsJson requires { keys: ["summary","founder",...], schema?: {..}, includeHeaders?: true|false }
 */
function GEMINI_BATCH_TABLE(optionsJson, prompt) {
  const a = Array.prototype.slice.call(arguments);
  if (a.length < 2) return [['⚠️ Usage: =GEMINI_BATCH_TABLE(optionsJson, "prompt", [cells...])']];
  let opts = {};
  try { opts = JSON.parse(String(optionsJson)); } catch (_) { return [['⚠️ optionsJson must be JSON']]; }
  const keys = Array.isArray(opts.keys) ? opts.keys : null;
  if (!keys || !keys.length) return [['⚠️ optionsJson.keys must be a non-empty array']];
  const jsonStr = GEMINI_BATCH_JSON(optionsJson, prompt, ...a.slice(2));
  let obj;
  try { obj = JSON.parse(String(jsonStr)); } catch (_) { return [['⚠️ Model did not return valid JSON']]; }

  const row = keys.map(k => (obj && obj[k] != null ? String(obj[k]) : ''));
  if (opts.includeHeaders) return [keys].concat([row]);
  return [row];
}

/* ----------------------------- JSONPATH -------------------------------- */
/** =JSONPATH(jsonString, "$.a[0].b") -> value or spill array */
function JSONPATH(jsonStr, path) {
  if (!jsonStr || !path) return '';
  let obj;
  try { obj = JSON.parse(String(jsonStr)); } catch (_) { return '⚠️ Invalid JSON'; }
  const tokens = parsePath_(String(path));
  let cur = [obj];
  for (const t of tokens) {
    const next = [];
    for (const val of cur) {
      if (t.type === 'prop') {
        if (val && Object.prototype.hasOwnProperty.call(val, t.key)) next.push(val[t.key]);
      } else if (t.type === 'index') {
        if (Array.isArray(val) && val.length > t.idx) next.push(val[t.idx]);
      }
    }
    cur = next;
  }
  if (cur.length === 0) return '';
  // If array of scalars → spill column; else stringify
  if (cur.length === 1) {
    const v = cur[0];
    if (Array.isArray(v)) return v.map(x => [toScalar_(x)]);
    return (typeof v === 'object') ? JSON.stringify(v) : toScalar_(v);
  }
  return cur.map(x => [toScalar_(x)]);
}
function parsePath_(p) {
  // basic $.a.b[0].c parser
  if (!p.startsWith('$')) throw new Error('Path must start with $');
  const out = [];
  let i = 1;
  while (i < p.length) {
    if (p[i] === '.') {
      i++;
      let key = '';
      while (i < p.length && /[A-Za-z0-9_\-]/.test(p[i])) { key += p[i++]; }
      if (!key) throw new Error('Bad path near dot');
      out.push({ type: 'prop', key });
    } else if (p[i] === '[') {
      i++;
      let num = '';
      while (i < p.length && /[0-9]/.test(p[i])) { num += p[i++]; }
      if (p[i] !== ']') throw new Error('Missing ]');
      i++;
      out.push({ type: 'index', idx: parseInt(num, 10) });
    } else if (p[i] === ' ') {
      i++;
    } else {
      throw new Error('Unexpected char in path: ' + p[i]);
    }
  }
  return out;
}
function toScalar_(v) {
  if (v == null) return '';
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return String(v);
  return JSON.stringify(v);
}

/* ------------------------------ BATCH WIZARD --------------------------- */
function showBatchDialog_() {
  const html = HtmlService.createHtmlOutputFromFile('Batch').setWidth(420).setHeight(600);
  SpreadsheetApp.getUi().showModalDialog(html, 'Gemini Batch Wizard');
}
function saveBatchConfig_(config) {
  // config = {sheetName, inputRange, outputStart, prompt, keysCsv, schemaJson, model, temperature, maxOutputTokens, search, schedule:"monthly"|"none", hour:9}
  PropertiesService.getScriptProperties().setProperty(PROP_BATCH, JSON.stringify(config || {}));
  return 'Saved';
}
function getBatchConfig_() {
  const raw = PropertiesService.getScriptProperties().getProperty(PROP_BATCH);
  return raw ? JSON.parse(raw) : null;
}

function getTriggerMeta_() {
  const raw = PropertiesService.getScriptProperties().getProperty(PROP_TRIGGER_META);
  try { return raw ? JSON.parse(raw) : {}; } catch (_) { return {}; }
}

function saveTriggerMeta_(meta) {
  PropertiesService.getScriptProperties().setProperty(PROP_TRIGGER_META, JSON.stringify(meta || {}));
}

function runBatchNow_() {
  const apiKey = getGeminiApiKey_();
  if (!apiKey) { SpreadsheetApp.getActive().toast('Set API key first.'); return; }
  const cfg = getBatchConfig_();
  if (!cfg) { SpreadsheetApp.getActive().toast('Open Batch wizard to configure.'); return; }

  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(cfg.sheetName);
  if (!sh) { SpreadsheetApp.getActive().toast('Sheet not found.'); return; }

  const inRange = sh.getRange(cfg.inputRange);
  const values = inRange.getValues(); // 2D
  const outStart = sh.getRange(cfg.outputStart);
  const outRow = outStart.getRow();
  const outCol = outStart.getColumn();
  const keys = cfg.keysCsv ? cfg.keysCsv.split(',').map(s => s.trim()).filter(Boolean) : [];
  const schema = parseMaybeJson_(cfg.schemaJson);
  const opts = {
    model: cfg.model || DEFAULT_MODEL,
    temperature: Number(cfg.temperature) || getDefaults_().temperature,
    maxOutputTokens: Number(cfg.maxOutputTokens) || getDefaults_().maxOutputTokens,
    search: !!cfg.search
  };
  if (schema) {
    opts.responseMimeType = 'application/json';
    opts.responseSchema = schema;
  }

  // Write headers if available
  if (keys.length) {
    sh.getRange(outRow, outCol, 1, keys.length).setValues([keys]);
  }

  // Iterate rows, one API call each
  for (let r = 0; r < values.length; r++) {
    const rowVals = values[r];
    const ctx = rowVals.join(' | ');
    const prompt = `${cfg.prompt}\n\nContext:\n${ctx}\n\n${schema ? 'Return ONLY JSON' : ''}`;

    const body = buildRequestBody_(prompt, opts);
    const json = callGemini_(body, opts.model, apiKey);

    let resultJSON = '';
    if (opts.responseMimeType === 'application/json') {
      // Find JSON text
      const cand = json && json.candidates && json.candidates[0];
      resultJSON = (cand && cand.content && cand.content.parts && cand.content.parts[0] && cand.content.parts[0].text) || '';
      if (!resultJSON) {
        const ec = extractFirstCandidate_(json);
        resultJSON = ec.text || '{}';
      }
      // Paste values by keys if provided
      if (keys.length) {
        let obj = {};
        try { obj = JSON.parse(resultJSON); } catch (_) {}
        const rowOut = keys.map(k => obj && obj[k] != null ? String(obj[k]) : '');
        sh.getRange(outRow + r + (keys.length ? 1 : 0), outCol, 1, keys.length).setValues([rowOut]);
      } else {
        sh.getRange(outRow + r, outCol).setValue(resultJSON);
      }
    } else {
      const { text } = extractFirstCandidate_(json);
      sh.getRange(outRow + r, outCol).setValue(text);
    }
    Utilities.sleep(100); // small pacing to avoid per-second limits
  }
  SpreadsheetApp.getActive().toast('Batch complete.');
}
function ensureMonthlyTrigger_() {
  const cfg = getBatchConfig_();
  if (!cfg) { SpreadsheetApp.getActive().toast('Open Batch wizard first.'); return; }

  const desiredHour = Number(cfg.hour) || 9;

  // Find existing triggers for our handler
  const all = ScriptApp.getProjectTriggers();
  const mine = all.filter(t => t.getHandlerFunction && t.getHandlerFunction() === 'runBatchNow_');

  // Load last-known metadata (because Apps Script cannot read a trigger's hour)
  const meta = getTriggerMeta_();
  const lastHour = (meta && typeof meta.hour === 'number') ? meta.hour : null;

  // If exactly one trigger exists and the hour hasn't changed → keep it
  if (mine.length === 1 && lastHour === desiredHour) {
    SpreadsheetApp.getActive().toast('Monthly trigger already exists; no changes made.');
    return;
  }

  // Else: remove all our triggers (cleanup duplicates or change hour)
  mine.forEach(t => ScriptApp.deleteTrigger(t));

  // Create exactly one new monthly trigger at the desired hour on the 1st of the month
  ScriptApp.newTrigger('runBatchNow_')
    .timeBased()
    .onMonthDay(1)
    .atHour(desiredHour)
    .create();

  // Persist metadata for future comparisons
  saveTriggerMeta_({ hour: desiredHour, updatedAt: new Date().toISOString() });

  SpreadsheetApp.getActive().toast('Monthly trigger set/updated.');
}

function parseMaybeJson_(s) {
  if (!s) return null;
  try { return JSON.parse(String(s)); } catch (_) { return null; }
}
