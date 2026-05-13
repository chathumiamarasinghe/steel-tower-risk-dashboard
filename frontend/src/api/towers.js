const BASE = (import.meta.env.VITE_API_URL || "http://localhost:8000").replace(
  /\/$/,
  ""
);

const geojsonCache = new Map();
const statsCache = { value: null, loaded: false };
const filterOptionsCache = { value: null, loaded: false };
const DOE_GEOJSON_CACHE_MAX = 32;
const doeGeojsonCache = new Map();

function doeCacheSet(url, data) {
  if (doeGeojsonCache.size >= DOE_GEOJSON_CACHE_MAX) {
    const firstKey = doeGeojsonCache.keys().next().value;
    doeGeojsonCache.delete(firstKey);
  }
  doeGeojsonCache.set(url, data);
}

function buildParams(filters, viewport) {
  const params = new URLSearchParams();
  if (!filters || typeof filters !== "object") return params;
  const keys = ["color", "state", "owner", "min_score", "max_score"];
  for (const key of keys) {
    const v = filters[key];
    if (v === undefined || v === null || v === "") continue;
    if (key === "min_score" || key === "max_score") {
      const n = Number(v);
      if (!Number.isFinite(n)) continue;
      params.set(key, String(n));
    } else {
      params.set(key, String(v));
    }
  }

  if (viewport && typeof viewport === "object") {
    const minLon = viewport.min_lon ?? viewport.minLon;
    const minLat = viewport.min_lat ?? viewport.minLat;
    const maxLon = viewport.max_lon ?? viewport.maxLon;
    const maxLat = viewport.max_lat ?? viewport.maxLat;
    const limit = viewport.limit ?? viewport.max ?? viewport.maxPoints;

    if (minLon != null) params.set("min_lon", String(minLon));
    if (minLat != null) params.set("min_lat", String(minLat));
    if (maxLon != null) params.set("max_lon", String(maxLon));
    if (maxLat != null) params.set("max_lat", String(maxLat));
    if (limit != null) params.set("limit", String(limit));
  }
  return params;
}

export async function fetchGeoJSON(filters, viewport) {
  const q = buildParams(filters, viewport);
  const url = `${BASE}/towers/geojson${q.toString() ? `?${q}` : ""}`;
  if (geojsonCache.has(url)) return geojsonCache.get(url);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`GeoJSON failed: ${res.status}`);
  const data = await res.json();
  geojsonCache.set(url, data);
  return data;
}

export async function fetchStats() {
  if (statsCache.loaded) return statsCache.value;
  const res = await fetch(`${BASE}/towers/stats`);
  if (!res.ok) throw new Error(`Stats failed: ${res.status}`);
  const data = await res.json();
  statsCache.value = data;
  statsCache.loaded = true;
  return data;
}

export async function fetchTower(id) {
  const enc = encodeURIComponent(id);
  const res = await fetch(`${BASE}/towers/${enc}`);
  if (!res.ok) throw new Error(`Tower failed: ${res.status}`);
  return res.json();
}

export async function fetchFilterOptions() {
  if (filterOptionsCache.loaded) return filterOptionsCache.value;
  const res = await fetch(`${BASE}/towers/filters/options`);
  if (!res.ok) throw new Error(`Filter options failed: ${res.status}`);
  const data = await res.json();
  filterOptionsCache.value = data;
  filterOptionsCache.loaded = true;
  return data;
}

export async function fetchDoeMatchGeoJSON(viewport) {
  const q = new URLSearchParams();
  if (viewport && typeof viewport === "object") {
    const minLon = viewport.min_lon ?? viewport.minLon;
    const minLat = viewport.min_lat ?? viewport.minLat;
    const maxLon = viewport.max_lon ?? viewport.maxLon;
    const maxLat = viewport.max_lat ?? viewport.maxLat;
    const limit = viewport.limit ?? viewport.max ?? viewport.maxPoints;

    if (minLon != null) q.set("min_lon", String(minLon));
    if (minLat != null) q.set("min_lat", String(minLat));
    if (maxLon != null) q.set("max_lon", String(maxLon));
    if (maxLat != null) q.set("max_lat", String(maxLat));
    if (limit != null) q.set("limit", String(limit));
  }

  const url = `${BASE}/towers/geojson/doe-match${q.toString() ? `?${q}` : ""}`;
  if (doeGeojsonCache.has(url)) return doeGeojsonCache.get(url);

  const res = await fetch(url);
  if (!res.ok) throw new Error(`DOE GeoJSON failed: ${res.status}`);
  const data = await res.json();
  if (
    !data ||
    data.type !== "FeatureCollection" ||
    !Array.isArray(data.features)
  ) {
    throw new Error("DOE GeoJSON response is invalid");
  }
  doeCacheSet(url, data);
  return data;
}

export async function downloadTowersCsv(filters) {
  const q = buildParams(filters);
  const url = `${BASE}/towers/export/csv${q.toString() ? `?${q}` : ""}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`CSV export failed: ${res.status}`);
  const blob = await res.blob();
  const a = document.createElement("a");
  const href = URL.createObjectURL(blob);
  a.href = href;
  a.download = "towers_export.csv";
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(href);
}

export function getTowerImageUrl(towerId, zoom = 18) {
  const params = new URLSearchParams({
    tower_id: String(towerId),
    zoom: String(zoom),
  });
  return `${BASE}/towers/image?${params.toString()}`;
}
