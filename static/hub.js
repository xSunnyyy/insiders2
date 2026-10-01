// Shared render helpers for the trending hub + list pages.
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const PALETTE = ["#6366f1", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444",
                 "#8b5cf6", "#14b8a6", "#f97316", "#64748b", "#ec4899"];

function fallbackLogo(sym) {
  let h = 0;
  for (const c of sym) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return `<span class="logo fallback" style="background:${PALETTE[h % PALETTE.length]}">${esc(sym.slice(0, 2))}</span>`;
}

function logoHtml(sym, url) {
  if (!url) return fallbackLogo(sym);
  return `<img class="logo" loading="lazy" alt="" data-sym="${esc(sym)}" src="${esc(url)}" onerror="logoFail(this)">`;
}

function logoFail(img) {
  img.outerHTML = fallbackLogo(img.dataset.sym || "?");
}

function ringHtml(score) {
  if (score == null) return `<span class="ring na" style="--v:0" title="Not enough data"><b>–</b></span>`;
  const cls = score >= 65 ? "hi" : score >= 45 ? "mid" : "lo";
  return `<span class="ring ${cls}" style="--v:${score}"><b>${score}</b></span>`;
}

function deltaHtml(pct) {
  if (pct == null || isNaN(pct)) return `<div class="delta flat">–</div>`;
  const cls = pct > 0 ? "up" : pct < 0 ? "down" : "flat";
  const arrow = pct > 0 ? "↗" : pct < 0 ? "↘" : "–";
  const v = Math.abs(pct);
  return `<div class="delta ${cls}">${arrow} ${v >= 100 ? v.toFixed(0) : +v.toFixed(1)}%</div>`;
}

function fmtPrice(p) {
  if (p == null) return "–";
  return "$" + (p >= 1000 ? p.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                          : p.toFixed(p < 1 ? 4 : 2));
}

function fmtNum(n) {
  if (n == null) return "–";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (Math.abs(n) >= 1e4) return (n / 1e3).toFixed(0) + "K";
  return n.toLocaleString();
}

function sparkSvg(points, w = 90, h = 28) {
  if (!points || points.length < 2) return "";
  const lo = Math.min(...points), hi = Math.max(...points), rng = hi - lo || 1;
  const step = w / (points.length - 1);
  const d = points.map((p, i) => `${(i * step).toFixed(1)},${(h - 2 - (p - lo) / rng * (h - 4)).toFixed(1)}`).join(" ");
  const up = points[points.length - 1] >= points[0];
  return `<svg class="spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true">
    <polyline fill="none" stroke="${up ? "var(--bull)" : "var(--bear)"}" stroke-width="1.6" points="${d}"/></svg>`;
}

function ago(sec) {
  if (sec == null) return "";
  if (sec < 60) return "just now";
  if (sec < 3600) return Math.round(sec / 60) + " min ago";
  return Math.round(sec / 3600) + " h ago";
}
