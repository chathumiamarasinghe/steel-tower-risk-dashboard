import { useEffect, useState } from "react";

import { getTowerImageUrl } from "../api/towers.js";

function fmt(v) {
  if (v === undefined || v === null || v === "") return "—";
  if (typeof v === "number" && !Number.isFinite(v)) return "—";
  return String(v);
}

function fmtNum(v, digits) {
  if (v === undefined || v === null || v === "") return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return digits !== undefined ? n.toFixed(digits) : String(n);
}

function fmtMoney(v) {
  if (v === undefined || v === null || v === "") return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function badgeFromColor(c) {
  const s = String(c || "");
  if (s === "#E24B4A") return { label: "High", bg: "#E24B4A", fg: "#fff" };
  if (s === "#EF9F27") return { label: "Moderate", bg: "#EF9F27", fg: "#111" };
  if (s === "green" || s === "#16a34a") return { label: "Low", bg: "#16a34a", fg: "#fff" };
  return { label: "—", bg: "#e5e7eb", fg: "#111" };
}

function Row({ label, value }) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: 12,
        padding: "8px 0",
        borderBottom: "1px solid #f1f5f9",
        fontSize: 13,
      }}
    >
      <span style={{ color: "#64748b", flexShrink: 0 }}>{label}</span>
      <span style={{ fontWeight: 600, color: "#0f172a", textAlign: "right", wordBreak: "break-word" }}>
        {value}
      </span>
    </div>
  );
}

function Section({ title, color, children }) {
  return (
    <div style={{ marginBottom: 18 }}>
      <div
        style={{
          fontSize: 12,
          fontWeight: 700,
          letterSpacing: "0.02em",
          color,
          marginBottom: 6,
          textTransform: "uppercase",
        }}
      >
        {title}
      </div>
      <div>{children}</div>
    </div>
  );
}

const ZOOM_TO_RESOLUTION_M = {
  20: "0.15",
  19: "0.30",
  18: "0.60",
  17: "1.19",
  16: "2.39",
  15: "4.78",
  14: "9.55",
};

function TowerImagePanel({ tower }) {
  const p = tower.properties || {};
  const towerId = p.id;
  const [zoom, setZoom] = useState(18);
  const [imageError, setImageError] = useState(false);
  const [imageLoading, setImageLoading] = useState(true);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    setZoom(18);
    setReloadKey(0);
  }, [towerId]);

  useEffect(() => {
    const missingId =
      towerId === undefined || towerId === null || String(towerId).trim() === "";
    if (missingId) {
      setImageLoading(false);
      setImageError(true);
      return;
    }
    setImageLoading(true);
    setImageError(false);
  }, [towerId, zoom]);

  const resolutionLabel = ZOOM_TO_RESOLUTION_M[zoom] ?? "—";
  const imageSrc =
    towerId !== undefined && towerId !== null && String(towerId) !== ""
      ? getTowerImageUrl(towerId, zoom)
      : "";

  const zoomButtons = [
    { label: "Area (z16)", z: 16 },
    { label: "Street (z17)", z: 17 },
    { label: "Tower (z18)", z: 18 },
    { label: "Detail (z20)", z: 20 },
  ];

  const btnBase = {
    flex: 1,
    minWidth: 0,
    padding: "8px 6px",
    fontSize: 11,
    fontWeight: 600,
    borderRadius: 6,
    cursor: "pointer",
    fontFamily: "inherit",
  };

  return (
    <div style={{ marginTop: 22, marginBottom: 18 }}>
      <div
        style={{
          background: "#185FA5",
          color: "#fff",
          padding: "10px 12px",
          borderRadius: "8px 8px 0 0",
          fontSize: 13,
          fontWeight: 700,
        }}
      >
        Aerial view — satellite imagery
      </div>
      <div
        style={{
          border: "1px solid #eee",
          borderTop: "none",
          borderRadius: "0 0 8px 8px",
          padding: 12,
          background: "#fff",
        }}
      >
        <div
          style={{
            position: "relative",
            width: "100%",
            aspectRatio: "560 / 320",
            borderRadius: 8,
            border: "1px solid #eee",
            overflow: "hidden",
            background: "#f1f5f9",
          }}
        >
          {imageError ? (
            <div
              style={{
                position: "absolute",
                inset: 0,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 12,
                padding: 16,
                background: "#e2e8f0",
                color: "#475569",
                fontSize: 13,
                textAlign: "center",
              }}
            >
              Satellite image unavailable for this location.
              <button
                type="button"
                onClick={() => {
                  setImageError(false);
                  setImageLoading(true);
                  setReloadKey((k) => k + 1);
                }}
                style={{
                  padding: "8px 14px",
                  borderRadius: 6,
                  border: "1px solid #185FA5",
                  background: "#fff",
                  color: "#185FA5",
                  fontWeight: 600,
                  cursor: "pointer",
                  fontFamily: "inherit",
                }}
              >
                Retry
              </button>
            </div>
          ) : (
            <>
              {imageSrc ? (
                <img
                  key={`${towerId}-${zoom}-${reloadKey}`}
                  src={imageSrc}
                  alt="Satellite view of tower location"
                  onLoad={() => {
                    setImageLoading(false);
                    setImageError(false);
                  }}
                  onError={() => {
                    setImageLoading(false);
                    setImageError(true);
                  }}
                  style={{
                    position: "absolute",
                    inset: 0,
                    width: "100%",
                    height: "100%",
                    objectFit: "cover",
                    display: "block",
                  }}
                />
              ) : null}
              {imageLoading && !imageError && (
                <div
                  style={{
                    position: "absolute",
                    inset: 0,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    background: "#e2e8f0",
                    color: "#64748b",
                    fontSize: 13,
                  }}
                >
                  Loading satellite view...
                </div>
              )}
            </>
          )}
        </div>

        <div style={{ marginTop: 10, fontSize: 13, color: "#334155" }}>
          Approx resolution: {resolutionLabel} m/pixel
        </div>

        <div style={{ display: "flex", gap: 8, marginTop: 12, flexWrap: "wrap" }}>
          {zoomButtons.map(({ label, z }) => {
            const active = zoom === z;
            return (
              <button
                key={z}
                type="button"
                onClick={() => setZoom(z)}
                style={{
                  ...btnBase,
                  background: active ? "#185FA5" : "#fff",
                  color: active ? "#fff" : "#185FA5",
                  border: active ? "1px solid #185FA5" : "1px solid #185FA5",
                }}
              >
                {label}
              </button>
            );
          })}
        </div>

        <p style={{ margin: "12px 0 0", fontSize: 11, color: "#64748b", lineHeight: 1.45 }}>
          Satellite imagery via Google Maps. Red marker shows tower coordinates. Images are not
          real-time — they reflect the most recent available satellite pass for this location.
        </p>
      </div>
    </div>
  );
}

export default function Sidebar({ tower, onClose }) {
  if (!tower) return null;

  const p = tower.properties || {};
  const badge = badgeFromColor(p.concern_color);

  return (
    <aside
      style={{
        position: "absolute",
        top: 0,
        right: 0,
        bottom: 0,
        width: 320,
        zIndex: 10,
        background: "#fff",
        boxShadow: "-4px 0 24px rgba(0,0,0,0.12)",
        display: "flex",
        flexDirection: "column",
        fontFamily: "system-ui, sans-serif",
      }}
    >
      <div
        style={{
          padding: "16px 18px",
          borderBottom: "1px solid #e2e8f0",
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: 12,
        }}
      >
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#0f172a", lineHeight: 1.3 }}>
            {fmt(p.owner)}
          </div>
          <div style={{ marginTop: 8, display: "flex", alignItems: "center", gap: 8 }}>
            <span
              style={{
                display: "inline-block",
                padding: "4px 10px",
                borderRadius: 999,
                fontSize: 12,
                fontWeight: 600,
                background: badge.bg,
                color: badge.fg,
              }}
            >
              {badge.label}
            </span>
          </div>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          style={{
            border: "none",
            background: "transparent",
            fontSize: 28,
            lineHeight: 1,
            cursor: "pointer",
            color: "#64748b",
            padding: 0,
            width: 36,
            height: 36,
          }}
        >
          ×
        </button>
      </div>

      <div
        style={{
          padding: "20px 18px",
          textAlign: "center",
          borderBottom: "1px solid #e2e8f0",
        }}
      >
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6 }}>Exposure score</div>
        <div
          style={{
            fontSize: 42,
            fontWeight: 800,
            color: badge.bg,
            lineHeight: 1,
          }}
        >
          {fmtNum(p.exposure_score, 4)}
        </div>
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: "16px 18px 24px" }}>
        <Section title="Tower info" color="#2563eb">
          <Row label="Voltage (kV)" value={fmtNum(p.voltage, 2)} />
          <Row label="State" value={fmt(p.state ?? p.worst_storm_state)} />
          <Row label="Status" value={fmt(p.status)} />
          <Row label="Structure" value={fmt(p.structure)} />
        </Section>

        <Section title="Weather signals" color="#d97706">
          <Row label="Max wind (m/s)" value={fmtNum(p.max_wind_speed, 2)} />
          <Row label="Max snow depth (m)" value={fmtNum(p.max_snow_depth, 3)} />
          <Row label="Nearest storm type" value={fmt(p.nearest_storm_type)} />
          <Row label="Storm event count" value={fmt(p.storm_event_count)} />
          <Row label="Max storm damage" value={fmtMoney(p.max_storm_damage_usd)} />
        </Section>

        <Section title="Outage signals" color="#dc2626">
          <Row label="DOE event count" value={fmt(p.doe_event_count)} />
          <Row label="Max customers affected" value={fmtNum(p.doe_max_customers, 0)} />
          <Row label="Max MW loss" value={fmtNum(p.doe_max_mw_loss, 2)} />
          <Row label="Outage type" value={fmt(p.doe_dominant_type)} />
          <Row label="NERC region" value={fmt(p.doe_nerc_region)} />
        </Section>

        <TowerImagePanel tower={tower} />
      </div>
    </aside>
  );
}
