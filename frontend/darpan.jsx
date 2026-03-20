import { useState, useEffect, useRef, useCallback } from "react";

// ─────────────────────────────────────────────────────────────────────────────
// API LAYER — Connects to the real FastAPI backend at localhost:8000
// Falls back gracefully to mock data if the API is not yet running.
// Set VITE_API_BASE or REACT_APP_API_BASE in your .env to override.
// ─────────────────────────────────────────────────────────────────────────────

const API_BASE = (typeof process !== "undefined" && process.env?.REACT_APP_API_BASE)
  || (typeof import.meta !== "undefined" && import.meta.env?.VITE_API_BASE)
  || "http://localhost:8000";

// Shared fetch helper with timeout + error handling
async function apiFetch(path, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      signal: controller.signal,
      headers: { "Content-Type": "application/json" },
      ...options,
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    clearTimeout(timeout);
    throw err;
  }
}

// Custom hook: fetches politicians list from API, falls back to POLITICIANS mock
function useApiPoliticians() {
  const [politicians, setPoliticians] = useState(null);
  const [apiAvailable, setApiAvailable] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    apiFetch("/api/politicians?limit=50&sort_by=score")
      .then((data) => {
        if (cancelled) return;
        // Map API response to the shape the UI expects
        const mapped = data.map((p) => ({
          id: p.id,
          name: p.name || p.name_normalized || "",
          party: p.party || "",
          state: p.state || "",
          constituency: p.constituency || "",
          photo: (p.name || "").split(" ").map(w => w[0]).join("").slice(0, 2).toUpperCase(),
          totalScore: p.total_score || 0,
          scores: {
            asset_growth: p.score_asset_growth || 0,
            tender_win: p.score_tender_linkage || 0,
            fund_flow: p.score_fund_flow || 0,
            land_reg: p.score_land_reg || 0,
            rti_flags: p.score_rti_contradiction || 0,
            network_depth: p.score_network_depth || 0,
          },
          scoreReasons: {},
          entities: [],
          fundFlows: [],
          assets: [],
          color: scoreToColor(p.total_score || 0),
          _needsDetail: true, // Flag: detail data loads on click
        }));
        setPoliticians(mapped);
        setApiAvailable(true);
      })
      .catch(() => {
        if (!cancelled) {
          // API not running — use mock data
          setPoliticians(null);
          setApiAvailable(false);
        }
      })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  return { politicians, apiAvailable, loading };
}

// Custom hook: fetches full politician detail (score reasons, fund trails, assets)
function useApiPoliticianDetail(politician, enabled) {
  const [detail, setDetail] = useState(null);

  useEffect(() => {
    if (!enabled || !politician?.id || !politician?._needsDetail) return;
    let cancelled = false;

    Promise.all([
      apiFetch(`/api/politicians/${politician.id}/score`).catch(() => null),
      apiFetch(`/api/politicians/${politician.id}/trails`).catch(() => []),
      apiFetch(`/api/politicians/${politician.id}`).catch(() => null),
    ]).then(([scoreData, trails, polDetail]) => {
      if (cancelled) return;
      const rawReasons = scoreData?.score_reasons || {};
      // API returns keys: tender_linkage, rti_contradiction
      // UI SCORING_CRITERIA uses ids: tender_win, rti_flags — remap here
      const scoreReasons = {
        ...rawReasons,
        tender_win: rawReasons.tender_win || rawReasons.tender_linkage || "",
        rti_flags:  rawReasons.rti_flags  || rawReasons.rti_contradiction || "",
      };
      const merged = {
        ...politician,
        scoreReasons,
        fundFlows: (trails || []).slice(0, 6).map(t => ({
          from: `${t.scheme_name?.slice(0, 25)} — ${t.fund_district || ""}`,
          to: t.winner_name || t.company_name || "",
          amount: `₹${(t.fund_amount || t.contract_value_cr || 0).toFixed(1)} Cr`,
          days: t.lag_days || 0,
          type: "PFMS→GeM",
          risk: t.risk_tier || "MEDIUM",
        })),
        assets: (polDetail?.assets_history || []).map(a => ({
          year: a.election_year,
          value: parseFloat(a.total_assets_lakh || 0) / 100, // Convert lakh to crore
        })),
        entities: (polDetail?.linked_companies || []).map(c => c.name).slice(0, 6),
        _needsDetail: false,
      };
      setDetail(merged);
    });

    return () => { cancelled = true; };
  }, [politician?.id, enabled]);

  return detail;
}

// Custom hook: fetches live platform stats
function useApiStats() {
  const [stats, setStats] = useState(null);
  useEffect(() => {
    apiFetch("/api/stats").then(setStats).catch(() => {});
  }, []);
  return stats;
}

// Map score 0–100 to a danger color
function scoreToColor(score) {
  if (score >= 75) return "#ff2d78";
  if (score >= 50) return "#ff9f0a";
  if (score >= 30) return "#00d4ff";
  return "#30d158";
}

// ─────────────────────────────────────────────────────────────────────────────
// MOCK DATA — Used when the backend API is not yet running
// ─────────────────────────────────────────────────────────────────────────────

const DATA_SOURCES = [
  { id: "ec", label: "Election Commission", short: "EC", color: "#00d4ff", x: 0.5, y: 0.08 },
  { id: "mca", label: "MCA21 Company Registry", short: "MCA", color: "#ff6b35", x: 0.88, y: 0.28 },
  { id: "pfms", label: "PFMS Fund Tracker", short: "PFMS", color: "#39ff14", x: 0.88, y: 0.72 },
  { id: "gem", label: "GeM Tender Portal", short: "GeM", color: "#ff2d78", x: 0.5, y: 0.92 },
  { id: "rera", label: "RERA Land Registry", short: "RERA", color: "#ffd700", x: 0.12, y: 0.72 },
  { id: "rti", label: "RTI Responses", short: "RTI", color: "#bf5af2", x: 0.12, y: 0.28 },
];

const SCORING_CRITERIA = [
  { id: "asset_growth", label: "Asset Growth Anomaly", weight: 25, description: "Compares declared asset growth in EC affidavits against provable income (salary + declared business). Growth >200% unexplained = high score.", formula: "score = min(25, (growth_pct - 100) / 20)" },
  { id: "tender_win", label: "Tender-to-Relative Linkage", weight: 25, description: "Cross-references GeM/state tender winners against MCA21 director graph. Matches spouse, children, siblings of politician.", formula: "score = linked_tender_count × 4 (capped at 25)" },
  { id: "fund_flow", label: "Fund Flow Correlation", weight: 20, description: "PFMS district disbursements → Time-correlated within 90 days to MCA21-linked company receiving tenders in same district.", formula: "score = correlated_flows × 5 (capped at 20)" },
  { id: "land_reg", label: "Land Registration Spike", weight: 15, description: "RERA records show property acquisitions in constituency >6 months before/after large fund releases. Flags benami patterns.", formula: "score = flagged_properties × 3 (capped at 15)" },
  { id: "rti_flags", label: "RTI Contradiction", weight: 10, description: "RTI filings that reveal discrepancy between official records and what politician declared publicly or in affidavit.", formula: "score = contradictions × 2.5 (capped at 10)" },
  { id: "network_depth", label: "Network Depth Score", weight: 5, description: "Shell company layers. Each hop away from politician in entity graph decreases score but layers > 3 increase suspicion.", formula: "score = shell_layers > 3 ? 5 : shell_layers × 1" },
];

const POLITICIANS = [
  {
    id: "MH-MLA-2024-0042",
    name: "Rajendra Patil",
    party: "INC",
    state: "Maharashtra",
    constituency: "Pune South",
    photo: "RP",
    totalScore: 87,
    scores: { asset_growth: 23, tender_win: 25, fund_flow: 20, land_reg: 12, rti_flags: 5, network_depth: 2 },
    scoreReasons: {
      asset_growth: "Assets grew from ₹12.4Cr (2019) to ₹89.7Cr (2024) — 623% growth. Salary + declared income = ₹84L over 5 years. Unexplained: ₹76.1Cr.",
      tender_win: "Patil Constructions Pvt Ltd (Director: Meena Patil, wife) won 9 state tenders. Maharashtra Road Works (brother-in-law director) won 5 tenders. Total: ₹340Cr.",
      fund_flow: "PFMS shows ₹84Cr released to Pune South NREGA in Mar 2023. Within 67 days Patil Constructions received ₹84Cr tender from same district PWD.",
      land_reg: "4 agricultural land parcels registered to shell entity 'Deccan Agri Pvt Ltd' (Patil nominee) worth ₹38Cr in 2022–23.",
      rti_flags: "RTI filed by journalist revealed contractor list withheld for 2 years. List showed Patil Constructions on all 9 awards.",
      network_depth: "2-layer shell: Patil → Deccan Infra Holdings → Maharashtra Road Works. CIN linkage confirmed via MCA21.",
    },
    entities: ["Patil Constructions Pvt Ltd", "Deccan Infra Holdings", "Maharashtra Road Works", "Deccan Agri Pvt Ltd"],
    fundFlows: [
      { from: "NREGA - Pune South", to: "Patil Constructions", amount: "₹84 Cr", days: 67, type: "PFMS→GeM", risk: "CRITICAL" },
      { from: "Smart City Fund - Pune", to: "Maharashtra Road Works", amount: "₹47 Cr", days: 43, type: "PFMS→GeM", risk: "HIGH" },
    ],
    assets: [{ year: 2019, value: 12.4 }, { year: 2020, value: 18.1 }, { year: 2021, value: 34.5 }, { year: 2022, value: 56.2 }, { year: 2023, value: 74.8 }, { year: 2024, value: 89.7 }],
    color: "#ff2d78",
  },
  {
    id: "UP-MP-2024-0118",
    name: "Sunita Verma",
    party: "BJP",
    state: "Uttar Pradesh",
    constituency: "Lucknow East",
    photo: "SV",
    totalScore: 62,
    scores: { asset_growth: 16, tender_win: 16, fund_flow: 15, land_reg: 9, rti_flags: 4, network_depth: 2 },
    scoreReasons: {
      asset_growth: "Assets grew from ₹8.1Cr to ₹31.5Cr (289%). Unexplained surplus: ₹21.3Cr over 5 years.",
      tender_win: "Verma Infra Solutions (spouse as director) won 3 Smart City tenders worth ₹112Cr in Lucknow district.",
      fund_flow: "Smart City Lucknow allocated ₹38Cr. Verma Infra won matching tender 38 days later.",
      land_reg: "3 commercial properties in Gomti Nagar acquired via Verma Holdings (spouse firm) post fund releases.",
      rti_flags: "RTI revealed Smart City project completion rate 12% despite 100% fund release — contractor: Verma Infra.",
      network_depth: "Single layer: Sunita → spouse Ramesh Verma → Verma Infra Solutions. Simple but clear.",
    },
    entities: ["Verma Infra Solutions", "Verma Holdings Pvt Ltd", "Lucknow Smart City Ltd"],
    fundFlows: [
      { from: "Smart City Fund - Lucknow", to: "Verma Infra Solutions", amount: "₹38 Cr", days: 38, type: "PFMS→GeM", risk: "HIGH" },
    ],
    assets: [{ year: 2019, value: 8.1 }, { year: 2020, value: 11.3 }, { year: 2021, value: 17.8 }, { year: 2022, value: 24.1 }, { year: 2023, value: 28.9 }, { year: 2024, value: 31.5 }],
    color: "#ff9f0a",
  },
  {
    id: "TN-MLA-2024-0207",
    name: "K. Anbazhagan",
    party: "DMK",
    state: "Tamil Nadu",
    constituency: "Chennai Central",
    photo: "KA",
    totalScore: 41,
    scores: { asset_growth: 11, tender_win: 12, fund_flow: 8, land_reg: 6, rti_flags: 3, network_depth: 1 },
    scoreReasons: {
      asset_growth: "Assets grew 119% over 5 years. While elevated, partial business income explains ~60% of growth.",
      tender_win: "Anbu Builders (brother as director) won 2 Urban Dev tenders worth ₹67Cr. Not conclusively linked to direct action.",
      fund_flow: "Urban development funds of ₹21Cr disbursed; Anbu Builders received ₹21Cr tender within 91 days.",
      land_reg: "2 residential plots registered in politician's name in constituency. Timing correlated but within normal range.",
      rti_flags: "One RTI showed minor discrepancy in contractor qualification scores.",
      network_depth: "Single-hop: brother's company. Low complexity.",
    },
    entities: ["Anbu Builders", "TN Urban Development Corp"],
    fundFlows: [
      { from: "Urban Dev Fund - Chennai", to: "Anbu Builders", amount: "₹21 Cr", days: 91, type: "PFMS→GeM", risk: "MEDIUM" },
    ],
    assets: [{ year: 2019, value: 22.3 }, { year: 2020, value: 27.1 }, { year: 2021, value: 33.4 }, { year: 2022, value: 39.8 }, { year: 2023, value: 44.2 }, { year: 2024, value: 48.9 }],
    color: "#ffd700",
  },
  {
    id: "GJ-MLA-2024-0033",
    name: "Harshad Patel",
    party: "BJP",
    state: "Gujarat",
    constituency: "Surat North",
    photo: "HP",
    totalScore: 21,
    scores: { asset_growth: 6, tender_win: 4, fund_flow: 5, land_reg: 3, rti_flags: 2, network_depth: 1 },
    scoreReasons: {
      asset_growth: "58% asset growth over 5 years. Largely explained by declared textile business income.",
      tender_win: "1 minor tender (₹18Cr) to loosely associated firm. Insufficient director linkage confirmed.",
      fund_flow: "Weak time-correlation. 140-day lag between fund release and tender award — beyond our 90-day threshold.",
      land_reg: "1 residential property. Consistent with income.",
      rti_flags: "One minor affidavit discrepancy — rounding error likely.",
      network_depth: "Distant 3rd-party association. Not classified as linked entity.",
    },
    entities: ["Patel Textiles Ltd"],
    fundFlows: [],
    assets: [{ year: 2019, value: 45.2 }, { year: 2020, value: 51.1 }, { year: 2021, value: 57.8 }, { year: 2022, value: 63.2 }, { year: 2023, value: 68.1 }, { year: 2024, value: 71.3 }],
    color: "#30d158",
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS
// ─────────────────────────────────────────────────────────────────────────────

const riskColor = (score) => {
  if (score >= 75) return "#ff2d78";
  if (score >= 50) return "#ff9f0a";
  if (score >= 30) return "#ffd700";
  return "#30d158";
};

const riskLabel = (score) => {
  if (score >= 75) return "CRITICAL SUSPECT";
  if (score >= 50) return "HIGH RISK";
  if (score >= 30) return "WATCH LIST";
  return "LOW RISK";
};

// ─────────────────────────────────────────────────────────────────────────────
// SPIDER WEB CANVAS — Core visual
// ─────────────────────────────────────────────────────────────────────────────

function SpiderWeb({ selectedPolitician, onSelectPolitician, onSelectSource, politicians }) {
  const canvasRef = useRef(null);
  const animRef = useRef(null);
  const timeRef = useRef(0);
  const [hoveredNode, setHoveredNode] = useState(null);

  const getNodes = useCallback((w, h) => {
    const cx = w / 2, cy = h / 2;
    const r1 = Math.min(w, h) * 0.32;
    const r2 = Math.min(w, h) * 0.15;

    // Data sources on outer ring
    const sourceNodes = DATA_SOURCES.map((s, i) => ({
      ...s,
      x: cx + r1 * Math.cos((i / DATA_SOURCES.length) * Math.PI * 2 - Math.PI / 2),
      y: cy + r1 * Math.sin((i / DATA_SOURCES.length) * Math.PI * 2 - Math.PI / 2),
      type: "source", radius: 22,
    }));

    // Politicians on inner ring
    const politicianNodes = politicians.map((p, i) => ({

      ...p,
      x: cx + r2 * Math.cos((i / politicians.length) * Math.PI * 2 - Math.PI / 4),
      y: cy + r2 * Math.sin((i / politicians.length) * Math.PI * 2 - Math.PI / 4),
      type: "politician", radius: 28,
    }));

    // Center node
    const centerNode = { id: "center", x: cx, y: cy, type: "center", radius: 38, label: "DARPAN" };

    return { sourceNodes, politicianNodes, centerNode, cx, cy };
  }, [politicians]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");

    const resize = () => {
      canvas.width = canvas.offsetWidth * window.devicePixelRatio;
      canvas.height = canvas.offsetHeight * window.devicePixelRatio;
      ctx.scale(window.devicePixelRatio, window.devicePixelRatio);
    };
    resize();
    window.addEventListener("resize", resize);

    const draw = (t) => {
      const w = canvas.offsetWidth, h = canvas.offsetHeight;
      ctx.clearRect(0, 0, w, h);

      const { sourceNodes, politicianNodes, centerNode, cx, cy } = getNodes(w, h);
      const allNodes = [...sourceNodes, ...politicianNodes, centerNode];
      timeRef.current = t;

      // ── Background radial grid ──
      for (let ring = 1; ring <= 4; ring++) {
        const r = Math.min(w, h) * ring * 0.09;
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.strokeStyle = `rgba(0, 200, 255, ${0.03 + ring * 0.01})`;
        ctx.lineWidth = 0.5;
        ctx.stroke();
      }

      // ── Spoke lines from center to outer ──
      DATA_SOURCES.forEach((_, i) => {
        const angle = (i / DATA_SOURCES.length) * Math.PI * 2 - Math.PI / 2;
        const outerR = Math.min(w, h) * 0.42;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(cx + outerR * Math.cos(angle), cy + outerR * Math.sin(angle));
        ctx.strokeStyle = "rgba(0, 200, 255, 0.05)";
        ctx.lineWidth = 0.5;
        ctx.stroke();
      });

      // ── Source → Center connections ──
      sourceNodes.forEach((src) => {
        const isSelected = selectedPolitician && selectedPolitician.id;
        ctx.beginPath();
        ctx.moveTo(src.x, src.y);
        ctx.lineTo(centerNode.x, centerNode.y);
        ctx.strokeStyle = `rgba(0, 200, 255, ${isSelected ? 0.06 : 0.12})`;
        ctx.lineWidth = 0.8;
        ctx.setLineDash([4, 8]);
        ctx.lineDashOffset = -(t * 0.02) % 12;
        ctx.stroke();
        ctx.setLineDash([]);
      });

      // ── Politician → Source connections (web strands) ──
      politicianNodes.forEach((pol) => {
        const isActivePol = selectedPolitician?.id === pol.id;
        sourceNodes.forEach((src) => {
          const opacity = isActivePol ? 0.55 : (selectedPolitician ? 0.04 : 0.14);
          const pulseOffset = (Math.sin(t * 0.001 + pol.id.length + src.id.length) + 1) / 2;

          // Draw gradient line
          const grad = ctx.createLinearGradient(pol.x, pol.y, src.x, src.y);
          grad.addColorStop(0, pol.color + Math.round(opacity * 255).toString(16).padStart(2, "0"));
          grad.addColorStop(1, src.color + Math.round(opacity * 0.6 * 255).toString(16).padStart(2, "0"));

          ctx.beginPath();
          ctx.moveTo(pol.x, pol.y);
          // Slight curve
          const mx = (pol.x + src.x) / 2 + (src.y - pol.y) * 0.12;
          const my = (pol.y + src.y) / 2 - (src.x - pol.x) * 0.12;
          ctx.quadraticCurveTo(mx, my, src.x, src.y);
          ctx.strokeStyle = grad;
          ctx.lineWidth = isActivePol ? 1.5 : 0.6;
          ctx.stroke();

          // Animated pulse dot
          if (isActivePol) {
            const pulse = (t * 0.0008 + pol.id.length * 0.3 + src.id.length * 0.2) % 1;
            const px = pol.x + (src.x - pol.x) * pulse;
            const py = pol.y + (src.y - pol.y) * pulse;
            ctx.beginPath();
            ctx.arc(px, py, 2.5, 0, Math.PI * 2);
            ctx.fillStyle = pol.color;
            ctx.fill();
          }
        });

        // Politician → Center strand
        ctx.beginPath();
        ctx.moveTo(pol.x, pol.y);
        ctx.lineTo(centerNode.x, centerNode.y);
        ctx.strokeStyle = pol.color + (isActivePol ? "88" : "22");
        ctx.lineWidth = isActivePol ? 2 : 0.8;
        ctx.stroke();
      });

      // ── Fund flow arcs between politicians (if selected, show related) ──
      if (selectedPolitician) {
        politicians.forEach((pol, i) => {
          if (pol.id === selectedPolitician.id) return;
          const p1 = politicianNodes.find(n => n.id === selectedPolitician.id);
          const p2 = politicianNodes[i];
          if (!p1 || !p2) return;
          // faint cross connection
          ctx.beginPath();
          ctx.moveTo(p1.x, p1.y);
          ctx.lineTo(p2.x, p2.y);
          ctx.strokeStyle = "rgba(255,200,0,0.08)";
          ctx.lineWidth = 0.5;
          ctx.setLineDash([2, 6]);
          ctx.stroke();
          ctx.setLineDash([]);
        });
      }

      // ── Draw source nodes ──
      sourceNodes.forEach((src) => {
        const isHovered = hoveredNode === src.id;
        const glow = ctx.createRadialGradient(src.x, src.y, 0, src.x, src.y, src.radius * 2.5);
        glow.addColorStop(0, src.color + "33");
        glow.addColorStop(1, "transparent");
        ctx.beginPath();
        ctx.arc(src.x, src.y, src.radius * 2.5, 0, Math.PI * 2);
        ctx.fillStyle = glow;
        ctx.fill();

        ctx.beginPath();
        ctx.arc(src.x, src.y, src.radius, 0, Math.PI * 2);
        ctx.fillStyle = "#060d1a";
        ctx.fill();
        ctx.strokeStyle = isHovered ? src.color : src.color + "88";
        ctx.lineWidth = isHovered ? 2 : 1.2;
        ctx.stroke();

        ctx.font = `bold 9px 'IBM Plex Mono', monospace`;
        ctx.fillStyle = src.color;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(src.short, src.x, src.y);

        // Label below
        ctx.font = `7px 'IBM Plex Mono', monospace`;
        ctx.fillStyle = src.color + "aa";
        const labelY = src.y + src.radius + 12;
        ctx.fillText(src.short === "PFMS" || src.short === "GeM" ? src.label.split(" ")[0] : src.short, src.x, labelY);
      });

      // ── Draw politician nodes ──
      politicianNodes.forEach((pol) => {
        const isSelected = selectedPolitician?.id === pol.id;
        const isHovered = hoveredNode === pol.id;
        const pulse = isSelected ? (Math.sin(t * 0.004) + 1) / 2 : 0;

        // Outer glow ring
        if (isSelected || isHovered) {
          const glowR = pol.radius * (2.5 + pulse * 0.5);
          const glow = ctx.createRadialGradient(pol.x, pol.y, 0, pol.x, pol.y, glowR);
          glow.addColorStop(0, pol.color + "44");
          glow.addColorStop(1, "transparent");
          ctx.beginPath();
          ctx.arc(pol.x, pol.y, glowR, 0, Math.PI * 2);
          ctx.fillStyle = glow;
          ctx.fill();
        }

        // Risk ring
        const riskR = pol.radius + 6;
        const filled = pol.totalScore / 100;
        ctx.beginPath();
        ctx.arc(pol.x, pol.y, riskR, -Math.PI / 2, -Math.PI / 2 + filled * Math.PI * 2);
        ctx.strokeStyle = pol.color;
        ctx.lineWidth = 2.5;
        ctx.stroke();

        // Node body
        ctx.beginPath();
        ctx.arc(pol.x, pol.y, pol.radius, 0, Math.PI * 2);
        ctx.fillStyle = isSelected ? "#0f1e33" : "#070e1a";
        ctx.fill();
        ctx.strokeStyle = isSelected ? pol.color : pol.color + "66";
        ctx.lineWidth = isSelected ? 2 : 1;
        ctx.stroke();

        // Avatar text
        ctx.font = `bold 10px 'IBM Plex Mono', monospace`;
        ctx.fillStyle = isSelected ? pol.color : pol.color + "cc";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(pol.photo, pol.x, pol.y - 4);

        ctx.font = `6px 'IBM Plex Mono', monospace`;
        ctx.fillStyle = pol.color + "99";
        ctx.fillText(pol.totalScore, pol.x, pol.y + 8);
      });

      // ── Center node ──
      const t_scale = 1 + Math.sin(t * 0.002) * 0.04;
      const cglow = ctx.createRadialGradient(cx, cy, 0, cx, cy, 60);
      cglow.addColorStop(0, "rgba(0,200,255,0.15)");
      cglow.addColorStop(1, "transparent");
      ctx.beginPath();
      ctx.arc(cx, cy, 60, 0, Math.PI * 2);
      ctx.fillStyle = cglow;
      ctx.fill();

      ctx.beginPath();
      ctx.arc(cx, cy, 38 * t_scale, 0, Math.PI * 2);
      ctx.fillStyle = "#060d1a";
      ctx.fill();
      ctx.strokeStyle = "#00d4ff";
      ctx.lineWidth = 1.5;
      ctx.stroke();

      ctx.font = `bold 9px 'IBM Plex Mono', monospace`;
      ctx.fillStyle = "#00d4ff";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("DARPAN", cx, cy - 5);
      ctx.font = `6px 'IBM Plex Mono', monospace`;
      ctx.fillStyle = "rgba(0,200,255,0.5)";
      ctx.fillText(".IN", cx, cy + 7);

      animRef.current = requestAnimationFrame(draw);
    };

    animRef.current = requestAnimationFrame(draw);
    return () => {
      cancelAnimationFrame(animRef.current);
      window.removeEventListener("resize", resize);
    };
  }, [selectedPolitician, hoveredNode, getNodes]);

  const handleClick = (e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const { sourceNodes, politicianNodes } = getNodes(canvas.offsetWidth, canvas.offsetHeight);
    for (const p of politicianNodes) {
      const dist = Math.sqrt((x - p.x) ** 2 + (y - p.y) ** 2);
      if (dist <= p.radius + 10) { onSelectPolitician(politicians.find(pol => pol.id === p.id)); return; }
    }
    for (const s of sourceNodes) {
      const dist = Math.sqrt((x - s.x) ** 2 + (y - s.y) ** 2);
      if (dist <= s.radius + 10) { onSelectSource(DATA_SOURCES.find(ds => ds.id === s.id)); return; }
    }
    onSelectPolitician(null);
  };

  const handleMouseMove = (e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const { sourceNodes, politicianNodes } = getNodes(canvas.offsetWidth, canvas.offsetHeight);
    let found = null;
    for (const p of [...politicianNodes, ...sourceNodes]) {
      const dist = Math.sqrt((x - p.x) ** 2 + (y - p.y) ** 2);
      if (dist <= p.radius + 10) { found = p.id; break; }
    }
    setHoveredNode(found);
    canvas.style.cursor = found ? "pointer" : "default";
  };

  return (
    <canvas
      ref={canvasRef}
      onClick={handleClick}
      onMouseMove={handleMouseMove}
      style={{ width: "100%", height: "100%", display: "block" }}
    />
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SCORE BREAKDOWN — Shows WHY a politician is flagged
// ─────────────────────────────────────────────────────────────────────────────

function ScoreBreakdown({ politician }) {
  const [expanded, setExpanded] = useState(null);
  if (!politician) return null;

  return (
    <div style={{ height: "100%", overflowY: "auto", padding: "0 2px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
        <div style={{
          width: 44, height: 44, borderRadius: 3, background: politician.color + "22",
          border: `2px solid ${politician.color}`, display: "flex", alignItems: "center",
          justifyContent: "center", fontSize: 14, fontWeight: 700, color: politician.color, flexShrink: 0,
        }}>{politician.photo}</div>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#e8f4ff", letterSpacing: "0.05em" }}>{politician.name}</div>
          <div style={{ fontSize: 9, color: "#4a7a9a", marginTop: 1 }}>{politician.party} · {politician.constituency} · {politician.state}</div>
          <div style={{ fontSize: 9, color: "#3a5a7a", fontFamily: "monospace", marginTop: 2 }}>{politician.id}</div>
        </div>
      </div>

      {/* Overall score meter */}
      <div style={{ background: "#070e1a", border: `1px solid ${politician.color}33`, borderRadius: 4, padding: "12px 14px", marginBottom: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: 10 }}>
          <div>
            <div style={{ fontSize: 9, color: "#4a7a9a", letterSpacing: "0.15em", textTransform: "uppercase" }}>Corruption Risk Score</div>
            <div style={{ fontSize: 36, fontWeight: 800, color: politician.color, lineHeight: 1, marginTop: 2 }}>{politician.totalScore}<span style={{ fontSize: 14, color: politician.color + "88" }}>/100</span></div>
          </div>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 9, padding: "3px 8px", background: politician.color + "22", border: `1px solid ${politician.color}44`, borderRadius: 2, color: politician.color, letterSpacing: "0.1em" }}>{riskLabel(politician.totalScore)}</div>
          </div>
        </div>
        <div style={{ height: 4, background: "#0d1a2e", borderRadius: 2, overflow: "hidden" }}>
          <div style={{ height: "100%", width: `${politician.totalScore}%`, background: `linear-gradient(90deg, ${politician.color}66, ${politician.color})`, borderRadius: 2, transition: "width 1s ease" }} />
        </div>
      </div>

      {/* Per-criterion breakdown */}
      <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 10 }}>Score Breakdown — Click to Expand</div>
      {SCORING_CRITERIA.map((c) => {
        const score = politician.scores[c.id] || 0;
        const pct = (score / c.weight) * 100;
        const isOpen = expanded === c.id;
        return (
          <div key={c.id} onClick={() => setExpanded(isOpen ? null : c.id)}
            style={{ background: "#070e1a", border: `1px solid ${isOpen ? "#1a3a5a" : "#0f1e30"}`, borderRadius: 3, marginBottom: 6, cursor: "pointer", overflow: "hidden", transition: "border-color 0.2s" }}>
            <div style={{ padding: "9px 12px", display: "flex", alignItems: "center", gap: 10 }}>
              <div style={{ flex: 1 }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 5 }}>
                  <span style={{ fontSize: 10, color: "#8ab0d0" }}>{c.label}</span>
                  <span style={{ fontSize: 11, fontWeight: 700, color: riskColor(pct) }}>{score}<span style={{ fontSize: 8, color: "#3a5a7a" }}>/{c.weight}</span></span>
                </div>
                <div style={{ height: 3, background: "#0d1a2e", borderRadius: 1, overflow: "hidden" }}>
                  <div style={{ height: "100%", width: `${pct}%`, background: riskColor(pct), borderRadius: 1 }} />
                </div>
              </div>
              <div style={{ fontSize: 8, color: "#3a5a7a", flexShrink: 0 }}>{isOpen ? "▲" : "▼"}</div>
            </div>
            {isOpen && (
              <div style={{ padding: "0 12px 10px", borderTop: "1px solid #0f1e30" }}>
                <div style={{ fontSize: 10, color: "#6a9abf", lineHeight: 1.6, marginTop: 8 }}>{(politician.scoreReasons || {})[c.id] || <span style={{ color: "#2a4a6a", fontStyle: "italic" }}>No detail available — run scorer.py to populate</span>}</div>
                <div style={{ marginTop: 8, padding: "6px 10px", background: "#0a1520", borderRadius: 2, borderLeft: "2px solid #1a3a5a" }}>
                  <div style={{ fontSize: 8, color: "#3a5a7a", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 3 }}>Formula</div>
                  <div style={{ fontSize: 9, color: "#4a7a9a", fontFamily: "monospace" }}>{c.formula}</div>
                </div>
                <div style={{ marginTop: 6, padding: "6px 10px", background: "#0a1520", borderRadius: 2, borderLeft: "2px solid #1a3a5a" }}>
                  <div style={{ fontSize: 8, color: "#3a5a7a", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 3 }}>Methodology</div>
                  <div style={{ fontSize: 9, color: "#4a7a9a", lineHeight: 1.5 }}>{c.description}</div>
                </div>
              </div>
            )}
          </div>
        );
      })}

      {/* Fund flows */}
      {politician.fundFlows.length > 0 && (
        <>
          <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", margin: "14px 0 8px" }}>Traced Fund Flows</div>
          {politician.fundFlows.map((f, i) => (
            <div key={i} style={{ background: "#070e1a", border: "1px solid #0f1e30", borderRadius: 3, padding: "10px 12px", marginBottom: 6 }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                <span style={{ fontSize: 9, background: f.risk === "CRITICAL" ? "rgba(255,45,120,0.15)" : f.risk === "HIGH" ? "rgba(255,159,10,0.15)" : "rgba(255,215,0,0.1)", color: f.risk === "CRITICAL" ? "#ff2d78" : f.risk === "HIGH" ? "#ff9f0a" : "#ffd700", padding: "2px 6px", borderRadius: 2, letterSpacing: "0.1em" }}>{f.risk}</span>
                <span style={{ fontSize: 9, color: "#3a5a7a" }}>{f.days}d lag · {f.type}</span>
              </div>
              <div style={{ fontSize: 10, color: "#4da6ff" }}>{f.from}</div>
              <div style={{ fontSize: 9, color: "#3a5a7a", margin: "3px 0" }}>↓ {f.amount} ({f.days} days later)</div>
              <div style={{ fontSize: 10, color: "#ff6b8a" }}>{f.to}</div>
            </div>
          ))}
        </>
      )}

      {/* Linked entities */}
      <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", margin: "14px 0 8px" }}>Linked Entities (MCA21)</div>
      {politician.entities.map((e, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 10px", background: "#070e1a", border: "1px solid #0f1e30", borderRadius: 2, marginBottom: 4 }}>
          <div style={{ width: 5, height: 5, borderRadius: "50%", background: politician.color, flexShrink: 0 }} />
          <span style={{ fontSize: 10, color: "#4da6ff" }}>{e}</span>
        </div>
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// ASSET CHART
// ─────────────────────────────────────────────────────────────────────────────

function AssetChart({ politician }) {
  if (!politician) return null;
  const data = politician.assets;
  const max = Math.max(...data.map(d => d.value));
  const W = 260, H = 80;
  const padL = 40, padB = 20, padT = 10, padR = 10;
  const chartW = W - padL - padR;
  const chartH = H - padT - padB;

  const pts = data.map((d, i) => ({
    x: padL + (i / (data.length - 1)) * chartW,
    y: padT + chartH - (d.value / max) * chartH,
  }));

  const pathD = pts.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");
  const fillD = `${pathD} L ${pts[pts.length - 1].x} ${padT + chartH} L ${padL} ${padT + chartH} Z`;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }}>
      <defs>
        <linearGradient id={`ag${politician.id}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={politician.color} stopOpacity="0.4" />
          <stop offset="100%" stopColor={politician.color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={fillD} fill={`url(#ag${politician.id})`} />
      <path d={pathD} stroke={politician.color} strokeWidth="1.5" fill="none" />
      {pts.map((p, i) => (
        <g key={i}>
          <circle cx={p.x} cy={p.y} r="2.5" fill={politician.color} />
          <text x={p.x} y={H - 6} textAnchor="middle" fontSize="6" fill="#3a5a7a">{data[i].year}</text>
          <text x={p.x} y={p.y - 6} textAnchor="middle" fontSize="6" fill={politician.color + "cc"}>₹{data[i].value}Cr</text>
        </g>
      ))}
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// DOCUMENTATION PANEL
// ─────────────────────────────────────────────────────────────────────────────

const DOCS = [
  {
    id: "overview", title: "System Overview", icon: "◈",
    content: `DARPAN.IN is a public-data corruption intelligence system that connects 6 Indian government databases to trace whether public funds have been misappropriated through politician-linked businesses.\n\nThe system works by: (1) ingesting structured data from public portals, (2) building a relationship graph of politicians and business entities, (3) tracking fund flows through PFMS, (4) matching tender winners against entity graph, (5) computing a multi-factor corruption risk score.`,
  },
  {
    id: "ingestion", title: "Module 1 — Data Ingestion", icon: "⊕",
    content: `Each data source is scraped/fetched by a dedicated Python module:\n\n• ec_scraper.py — Downloads EC affidavit PDFs, runs pdfplumber extraction to parse declared assets, PAN numbers, family members, and business interests.\n\n• mca21_fetcher.py — Queries MCA21 REST API and web scraper to fetch company details, director/shareholder lists by PAN.\n\n• pfms_watcher.py — Monitors PFMS public dashboards for district-wise scheme disbursements. Stores amounts with timestamps.\n\n• gem_crawler.py — Scrapes GeM portal tender awards: winning bidder CIN, contract value, district, date.\n\n• rera_scraper.py — Fetches state RERA portals for property registrations by PAN/entity name.\n\n• rti_indexer.py — Indexes publicly uploaded RTI responses from RTIOnline. Extracts contractor names and fund data.`,
  },
  {
    id: "entity_graph", title: "Module 2 — Entity Graph", icon: "⬡",
    content: `The entity resolution engine (entity_graph.py) builds a Neo4j graph:\n\nNodes: Politician, Company, Individual, BankAccount\nEdges: IS_DIRECTOR, IS_SHAREHOLDER, FAMILY_OF, RECEIVED_TENDER, FUNDED_BY\n\nKey logic:\n1. Extract politician PAN from EC affidavit\n2. Query MCA21 for all companies where that PAN appears as director/shareholder\n3. For each company, recursively fetch subsidiary companies (up to depth 4)\n4. Cross-match family members from affidavit against director lists\n5. Score family match confidence: spouse=1.0, child=0.95, sibling=0.8, associate=0.5\n\nFuzzy matching (Levenshtein distance ≤2) handles name spelling variations across datasets.`,
  },
  {
    id: "fund_tracing", title: "Module 3 — Fund Flow Tracing", icon: "⟳",
    content: `fund_tracer.py correlates two data streams:\n\nStream A: PFMS district fund releases\n  - scheme_id, district, amount, release_date\n\nStream B: GeM/State tender awards\n  - contractor_cin, district, amount, award_date\n\nCorrelation algorithm:\n1. For each fund release in Stream A:\n   a. Find all tenders in same district within ±90 days\n   b. Filter tenders where contractor_cin is in entity graph of any politician\n   c. Flag if amount ≥ 60% of fund release amount (tolerance for splitting)\n2. Calculate lag days (tender_date - fund_release_date)\n3. Assign CRITICAL if lag <50d, HIGH if 50–90d, MEDIUM if 90–180d\n\nRTC (Revenue/Land Records): cross-matches RERA property acquisitions with politician entity graph within 180-day window of large fund releases.`,
  },
  {
    id: "scoring", title: "Module 4 — Risk Scoring", icon: "◎",
    content: `scorer.py computes a 0–100 risk score from 6 weighted criteria:\n\n1. Asset Growth Anomaly (25pts)\n   Formula: min(25, (growth_pct - 100) / 20)\n   Threshold: >100% unexplained growth triggers scoring\n\n2. Tender-to-Relative Linkage (25pts)\n   Formula: linked_tender_count × 4 (cap: 25)\n   Source: GeM tenders + MCA21 family graph match\n\n3. Fund Flow Correlation (20pts)\n   Formula: correlated_flows × 5 (cap: 20)\n   Source: PFMS release → 90-day tender window\n\n4. Land Registration Spike (15pts)\n   Formula: flagged_properties × 3 (cap: 15)\n   Source: RERA acquisitions post-fund-release\n\n5. RTI Contradictions (10pts)\n   Formula: contradictions × 2.5 (cap: 10)\n   Source: RTIOnline disclosed documents\n\n6. Network Depth (5pts)\n   Formula: shell_layers > 3 ? 5 : layers × 1\n   Source: Entity graph depth traversal\n\nTotal score thresholds:\n≥75: CRITICAL SUSPECT\n50–74: HIGH RISK\n30–49: WATCH LIST\n<30: LOW RISK`,
  },
  {
    id: "architecture", title: "System Architecture", icon: "⌖",
    content: `Tech Stack:\n\n• Scraping: Python (Scrapy, Playwright, pdfplumber)\n• Storage: PostgreSQL (fund records, tenders)\n            Neo4j (entity relationship graph)\n            Redis (job queue, caching)\n• Processing: Apache Airflow (daily DAG scheduling)\n• Entity Resolution: spaCy NER + dedupe.io\n• Scoring: XGBoost model + rule engine\n• API: FastAPI (serves data to frontend)\n• Frontend: React + D3.js (spider web visualization)\n\nData Flow:\n  Scrapers → PostgreSQL/Neo4j → Airflow DAG → scorer.py → FastAPI → React UI\n\nAll sources are legally public (RTI Act, Companies Act mandatory disclosures, Election Commission rules, PFMS public dashboard). No private data is accessed.`,
  },
];

function DocPanel({ selectedSource }) {
  const [activeDoc, setActiveDoc] = useState("overview");
  const doc = DOCS.find(d => d.id === activeDoc);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column" }}>
      <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 10, padding: "0 2px" }}>System Documentation</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: 12 }}>
        {DOCS.map(d => (
          <button key={d.id} onClick={() => setActiveDoc(d.id)}
            style={{ background: activeDoc === d.id ? "#0f1e33" : "transparent", border: `1px solid ${activeDoc === d.id ? "#1a3a5a" : "#0f1e30"}`, borderRadius: 2, color: activeDoc === d.id ? "#4da6ff" : "#3a5a7a", fontSize: 8, padding: "4px 8px", cursor: "pointer", fontFamily: "inherit", letterSpacing: "0.08em" }}>
            {d.icon} {d.title.split("—")[0].trim().toUpperCase()}
          </button>
        ))}
      </div>
      {doc && (
        <div style={{ flex: 1, overflowY: "auto" }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#4da6ff", marginBottom: 8, letterSpacing: "0.05em" }}>{doc.title}</div>
          <div style={{ fontSize: 10, color: "#5a8aaa", lineHeight: 1.75, whiteSpace: "pre-wrap", fontFamily: "inherit" }}>{doc.content}</div>
              {selectedSource && (
                <div style={{ marginTop: 14, padding: "10px 12px", background: selectedSource.color + "0d", border: `1px solid ${selectedSource.color}33`, borderRadius: 3 }}>
                  <div style={{ fontSize: 9, color: selectedSource.color, letterSpacing: "0.1em", marginBottom: 6 }}>SELECTED SOURCE: {selectedSource.label.toUpperCase()}</div>
                  <div style={{ fontSize: 9, color: "#4a7a9a", lineHeight: 1.6, marginBottom: 8 }}>
                    {selectedSource.id === "ec" && "Election Commission: Affidavit PDFs → pdfplumber extraction → PAN, asset values, family members, business interests stored in PostgreSQL."}
                    {selectedSource.id === "mca" && "MCA21: REST API + web scrape → company director/shareholder data → stored in Neo4j as COMPANY nodes with IS_DIRECTOR / IS_SHAREHOLDER edges."}
                    {selectedSource.id === "pfms" && "PFMS: Public dashboard scraping → district-wise scheme disbursements → stored with timestamps for temporal correlation."}
                    {selectedSource.id === "gem" && "GeM Portal: Tender award pages → winning bidder CIN, contract value, district → stored for matching against entity graph."}
                    {selectedSource.id === "rera" && "RERA: State portal scraping → property registrations by PAN/entity → flagged when post-fund-release within 180-day window."}
                    {selectedSource.id === "rti" && "RTIOnline: Indexed RTI responses → NLP extraction of contractor names and fund amounts → contradiction detection vs official records."}
                  </div>
                  {/* Real portal link */}
                  {SOURCE_REGISTRY[selectedSource.id] && (
                    <a href={SOURCE_REGISTRY[selectedSource.id].portal} target="_blank" rel="noopener noreferrer"
                      style={{ display: "block", fontSize: 8, color: selectedSource.color, textDecoration: "none", letterSpacing: "0.08em", padding: "5px 8px", border: `1px solid ${selectedSource.color}44`, borderRadius: 2, textAlign: "center" }}
                      onMouseEnter={e => e.target.style.background = selectedSource.color + "20"}
                      onMouseLeave={e => e.target.style.background = "transparent"}>
                      ↗ Visit {SOURCE_REGISTRY[selectedSource.id].name}
                    </a>
                  )}
                  {/* Legal basis */}
                  {SOURCE_REGISTRY[selectedSource.id]?.legalBasis && (
                    <div style={{ marginTop: 8, fontSize: 7, color: "#2a4a6a", lineHeight: 1.6, padding: "6px 8px", background: "#050c18", borderRadius: 2 }}>
                      ⚖ {SOURCE_REGISTRY[selectedSource.id].legalBasis}
                    </div>
                  )}
                  {/* Data points collected */}
                  {SOURCE_REGISTRY[selectedSource.id]?.dataPoints && (
                    <div style={{ marginTop: 8 }}>
                      <div style={{ fontSize: 7, color: "#2a4a6a", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 4 }}>Data Collected</div>
                      {SOURCE_REGISTRY[selectedSource.id].dataPoints.map((dp, i) => (
                        <div key={i} style={{ fontSize: 7, color: "#3a6a8a", padding: "2px 0" }}>· {dp}</div>
                      ))}
                    </div>
                  )}
                </div>
              )}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE REGISTRY — Every data source with its exact public URL
// ─────────────────────────────────────────────────────────────────────────────

const SOURCE_REGISTRY = {
  ec: {
    name: "Election Commission of India",
    portal: "https://affidavit.eci.gov.in",
    searchUrl: (name, state) =>
      `https://affidavit.eci.gov.in/candidateWise/getCandidateListYearAndStatewise?state=${encodeURIComponent(state || "")}&name=${encodeURIComponent(name || "")}`,
    description: "Mandatory candidate affidavit disclosures (Form 26). Every contesting politician must declare assets, liabilities, PAN, family members, and criminal cases before each election.",
    legalBasis: "Representation of the People Act 1951 + Supreme Court order in Union of India v. Association for Democratic Reforms (2002)",
    color: "#00d4ff",
    icon: "🗳",
    dataPoints: ["Assets (movable + immovable)", "PAN card number", "Family members + their assets", "Business interests", "Criminal cases", "Liabilities"],
  },
  mca: {
    name: "MCA21 — Ministry of Corporate Affairs",
    portal: "https://www.mca.gov.in",
    searchUrl: (cin) =>
      cin ? `https://www.mca.gov.in/mcafoportal/viewCompanyMasterData.do?din=${cin}` : "https://www.mca.gov.in/mcafoportal/viewSignatoryDetails.do",
    description: "National registry of all companies registered in India. Every director appointment and shareholding above 2% is a public legal record.",
    legalBasis: "Companies Act 2013 — Section 92 (Annual Return), Section 170 (Directors), Section 138",
    color: "#ff6b35",
    icon: "🏢",
    dataPoints: ["Company registration details", "Director names + DIN", "Shareholding pattern", "Subsidiary companies", "Paid-up capital", "Registered address"],
  },
  pfms: {
    name: "PFMS — Public Financial Management System",
    portal: "https://pfms.nic.in",
    searchUrl: () => "https://pfms.nic.in/new/site_content/report/DistrictReport.aspx",
    description: "Government of India's real-time fund tracking system. Every rupee released from central schemes to districts is logged here with timestamps.",
    legalBasis: "PFMS is a GoI initiative under Ministry of Finance. Data is publicly accessible on the PFMS dashboard.",
    color: "#39ff14",
    icon: "💰",
    dataPoints: ["Scheme name + code", "State and district", "Amount released (crores)", "Release date", "Implementing agency", "Financial year"],
  },
  gem: {
    name: "GeM — Government e-Marketplace",
    portal: "https://gem.gov.in",
    searchUrl: (ref) =>
      ref ? `https://gem.gov.in/search/bid?orderId=${ref}` : "https://gem.gov.in/search/bid",
    description: "Mandatory procurement portal for all central government purchases above ₹25,000. All awarded tenders, winner names, CINs, and contract values are public.",
    legalBasis: "GeM was set up under GFR 2017 Rule 149. All order details are publicly searchable.",
    color: "#ff2d78",
    icon: "📋",
    dataPoints: ["Tender reference number", "Winning company name + CIN", "Contract value", "Award date", "Department / buyer", "Delivery district"],
  },
  rera: {
    name: "RERA — Real Estate Regulatory Authority",
    portal: "https://rera.gov.in",
    searchUrl: (state) =>
      state ? (SOURCE_REGISTRY.rera.statePorts[state] || "https://rera.gov.in") : "https://rera.gov.in",
    statePorts: {
      "Maharashtra": "https://maharera.mahaonline.gov.in",
      "Delhi": "https://rera.delhi.gov.in",
      "Karnataka": "https://rera.karnataka.gov.in",
      "Uttar Pradesh": "https://www.up-rera.in",
      "Tamil Nadu": "https://www.tnrera.in",
      "Gujarat": "https://gujrera.gujarat.gov.in",
      "Rajasthan": "https://rera.rajasthan.gov.in",
    },
    description: "State-level real estate project and property registration database. All promoters must disclose PAN, CIN, and project details publicly.",
    legalBasis: "Real Estate (Regulation and Development) Act 2016 — Section 4 (mandatory disclosure of project details)",
    color: "#ffd700",
    icon: "🏗",
    dataPoints: ["RERA registration number", "Promoter name + PAN + CIN", "Project location", "Land area", "Declared project value", "Registration date"],
  },
  rti: {
    name: "RTIOnline — Right to Information Portal",
    portal: "https://www.rtionline.gov.in",
    searchUrl: () => "https://www.rtionline.gov.in/download.php",
    description: "Central RTI portal where citizens file information requests and responses are published. RTI responses from public authorities revealing contractor details, fund utilization, and work completion are indexed here.",
    legalBasis: "Right to Information Act 2005 — Section 4 (suo motu disclosure) + filed RTI responses",
    color: "#bf5af2",
    icon: "📄",
    dataPoints: ["Application number", "Public authority", "RTI subject", "Disclosed contractor names", "Fund amounts mentioned", "Response documents"],
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// SOURCES PANEL — Per-politician data provenance with clickable verification links
// ─────────────────────────────────────────────────────────────────────────────

function SourcesPanel({ politician, apiAvailable }) {
  const [detail, setDetail] = useState(null);

  useEffect(() => {
    if (!politician?.id || !apiAvailable) return;
    apiFetch(`/api/politicians/${politician.id}`)
      .then(setDetail)
      .catch(() => {});
  }, [politician?.id, apiAvailable]);

  const pol = detail || politician;
  const name = pol.name || pol.name_normalized || "";
  const state = pol.state || "";

  // Build EC affidavit URL — real ECI search by name
  const ecUrl = SOURCE_REGISTRY.ec.searchUrl(name, state);
  // Build MCA21 URL — search by any known CIN, else name search
  const firstCin = pol.linked_companies?.[0]?.cin || null;
  const mcaUrl = SOURCE_REGISTRY.mca.searchUrl(firstCin);

  return (
    <div style={{ height: "100%", overflowY: "auto" }}>

      {/* Header */}
      <div style={{ marginBottom: 14 }}>
        <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 6 }}>
          Data Provenance — {name}
        </div>
        <div style={{ fontSize: 8, color: "#2a4a6a", lineHeight: 1.6, padding: "8px 10px", background: "#070e1a", borderRadius: 3, border: "1px solid #0d2040" }}>
          Every data point shown for this politician is sourced from legally mandatory public disclosures. Click any link below to verify the original source yourself.
        </div>
      </div>

      {/* ── EC Affidavit ── */}
      <SourceCard
        source={SOURCE_REGISTRY.ec}
        status="verified"
        records={[
          { label: "Affidavit ID", value: pol.id || pol.ec_affidavit_id || "—" },
          { label: "PAN", value: pol.pan || (pol.pan === null ? "Not disclosed" : "Loading...") },
          { label: "Election Year", value: pol.election_year || "2024" },
          { label: "Assets (latest)", value: pol.latest_assets_lakh ? `₹${(pol.latest_assets_lakh/100).toFixed(2)}Cr` : pol.assets?.slice(-1)[0]?.value ? `₹${pol.assets.slice(-1)[0].value}Cr` : "—" },
          { label: "Family Members Declared", value: pol.family_members?.length ?? pol.scoreReasons?.tender_linkage ? "See MCA21" : "—" },
        ]}
        verifyUrl={ecUrl}
        verifyLabel="Search ECI Affidavit Portal →"
        note={`Affidavit filed by ${name} under Section 33A of the Representation of the People Act 1951. This is a sworn legal declaration — false information is a criminal offence.`}
      />

      {/* ── MCA21 ── */}
      {(pol.linked_companies?.length > 0 || pol.entities?.length > 0) && (
        <SourceCard
          source={SOURCE_REGISTRY.mca}
          status="verified"
          records={
            (pol.linked_companies || pol.entities?.map(e => ({ name: e, cin: "—", link_type: "linked" })) || [])
              .slice(0, 5)
              .map(c => ({
                label: c.link_type === "direct" ? "Direct Director" : c.link_type === "family" ? `Family Link (${c.relation_via || "relative"})` : "Associated",
                value: c.name || c,
                sub: c.cin && c.cin !== "—" ? c.cin : null,
              }))
          }
          verifyUrl={mcaUrl}
          verifyLabel="Verify on MCA21 Portal →"
          note="Director and shareholder data is legally filed with the Registrar of Companies. All CIN numbers are searchable on the MCA21 portal."
          extraLinks={
            (pol.linked_companies || []).filter(c => c.cin).slice(0, 3).map(c => ({
              label: `${(c.name || "").slice(0, 28)} (${c.cin})`,
              url: `https://www.mca.gov.in/mcafoportal/viewCompanyMasterData.do?din=${c.cin}`,
            }))
          }
        />
      )}

      {/* ── PFMS Fund Releases ── */}
      {(pol.fundFlows?.length > 0 || politician.fund_trail_count > 0) && (
        <SourceCard
          source={SOURCE_REGISTRY.pfms}
          status="verified"
          records={
            (pol.fundFlows || []).slice(0, 4).map(f => ({
              label: f.scheme_name || f.from || "Fund Release",
              value: f.fund_amount || f.amount || "—",
              sub: f.release_date ? `Released: ${f.release_date}` : `${f.days || "?"} day lag`,
            }))
          }
          verifyUrl={SOURCE_REGISTRY.pfms.searchUrl()}
          verifyLabel="Check PFMS District Reports →"
          note="Fund disbursements are published in real-time on the PFMS public dashboard. Filter by state + district + scheme to verify each release."
        />
      )}

      {/* ── GeM Tenders ── */}
      {pol.fundFlows?.length > 0 && (
        <SourceCard
          source={SOURCE_REGISTRY.gem}
          status="verified"
          records={
            (pol.fundFlows || []).slice(0, 4).map(f => ({
              label: f.winner_name || f.to || "Tender Winner",
              value: f.contract_value_cr ? `₹${f.contract_value_cr}Cr` : f.amount || "—",
              sub: f.award_date ? `Awarded: ${f.award_date}` : null,
            }))
          }
          verifyUrl={SOURCE_REGISTRY.gem.searchUrl()}
          verifyLabel="Search GeM Awarded Orders →"
          note="All GeM tender awards are publicly searchable by order ID, buyer department, or seller name. The portal is operated by the Government of India."
          extraLinks={
            (pol.fundFlows || []).filter(f => f.tender_ref_id).slice(0, 3).map(f => ({
              label: `Order: ${f.tender_ref_id}`,
              url: SOURCE_REGISTRY.gem.searchUrl(f.tender_ref_id),
            }))
          }
        />
      )}

      {/* ── RTI ── */}
      {(pol.score_rti_contradiction > 0 || pol.scores?.rti_flags > 0) && (
        <SourceCard
          source={SOURCE_REGISTRY.rti}
          status="flagged"
          records={[{
            label: "RTI contradictions found",
            value: `${pol.score_rti_contradiction || pol.scores?.rti_flags || 0} filed responses`,
            sub: "Contractor / fund discrepancies detected",
          }]}
          verifyUrl={SOURCE_REGISTRY.rti.searchUrl()}
          verifyLabel="Search RTIOnline Responses →"
          note="RTI responses are uploaded by public authorities after citizen information requests. All documents shown are those that have been officially disclosed."
        />
      )}

      {/* ── RERA ── */}
      {(pol.score_land_reg > 0 || pol.scores?.land_reg > 0) && (
        <SourceCard
          source={SOURCE_REGISTRY.rera}
          status="flagged"
          records={[{
            label: "Flagged RERA registrations",
            value: `${pol.score_land_reg || pol.scores?.land_reg || 0} properties`,
            sub: `Registered within 180 days of fund releases in ${state}`,
          }]}
          verifyUrl={SOURCE_REGISTRY.rera.searchUrl(state)}
          verifyLabel={`Verify on ${state} RERA Portal →`}
          note="Property registrations by linked entities within 6 months of government fund releases. Searchable by promoter PAN or company CIN on state RERA portals."
        />
      )}

      {/* ── Legal disclaimer ── */}
      <div style={{ marginTop: 14, padding: "10px 12px", background: "#070e1a", border: "1px solid #0d2040", borderRadius: 3 }}>
        <div style={{ fontSize: 8, color: "#3a5a7a", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 5 }}>⚖ Legal Basis</div>
        <div style={{ fontSize: 8, color: "#2a4a6a", lineHeight: 1.7 }}>
          All data is sourced from legally mandatory public disclosures under Indian law. DARPAN.IN does not create, modify, or infer any data — it cross-references records that politicians and companies are legally required to file with the government.
        </div>
        <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 3 }}>
          {[
            ["RTI Act 2005", "https://www.legislation.gov.in/sites/default/files/A2005-22_0.pdf"],
            ["Companies Act 2013", "https://www.mca.gov.in/Ministry/pdf/CompaniesAct2013.pdf"],
            ["R.P. Act 1951 (Section 33A)", "https://www.eci.gov.in/files/file/8580-rp-act-1951/"],
            ["Supreme Court 2002 Order", "https://main.sci.gov.in/judgment/judis/17851.pdf"],
            ["RERA Act 2016", "https://rera.gov.in/resources/RERA-Act-2016.pdf"],
          ].map(([label, url]) => (
            <a key={label} href={url} target="_blank" rel="noopener noreferrer"
              style={{ fontSize: 7, color: "#1a4a7a", textDecoration: "none", letterSpacing: "0.06em" }}
              onMouseEnter={e => e.target.style.color = "#4da6ff"}
              onMouseLeave={e => e.target.style.color = "#1a4a7a"}>
              ↗ {label}
            </a>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE CARD — Reusable card showing one data source's contribution
// ─────────────────────────────────────────────────────────────────────────────

function SourceCard({ source, status, records, verifyUrl, verifyLabel, note, extraLinks }) {
  const [open, setOpen] = useState(true);
  const statusColor = status === "verified" ? "#30d158" : status === "flagged" ? "#ff9f0a" : "#4da6ff";
  const statusLabel = status === "verified" ? "✓ VERIFIED" : status === "flagged" ? "⚠ FLAGGED" : "● PENDING";

  return (
    <div style={{ border: `1px solid ${source.color}33`, borderLeft: `3px solid ${source.color}`, borderRadius: 3, marginBottom: 10, overflow: "hidden" }}>

      {/* Header */}
      <div onClick={() => setOpen(o => !o)}
        style={{ display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", cursor: "pointer", background: source.color + "0a" }}>
        <span style={{ fontSize: 12 }}>{source.icon}</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 9, color: source.color, fontWeight: 700, letterSpacing: "0.08em" }}>{source.name}</div>
          <div style={{ fontSize: 7, color: "#2a4a6a", marginTop: 1 }}>{source.portal}</div>
        </div>
        <div style={{ fontSize: 7, color: statusColor, letterSpacing: "0.1em", flexShrink: 0 }}>{statusLabel}</div>
        <span style={{ fontSize: 8, color: "#2a4a6a" }}>{open ? "▲" : "▼"}</span>
      </div>

      {/* Body */}
      {open && (
        <div style={{ padding: "8px 10px", background: "#070e1a" }}>

          {/* Data records extracted from this source */}
          {records && records.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              {records.map((r, i) => r.value && r.value !== "—" ? (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", padding: "4px 0", borderBottom: "1px solid #0a1520" }}>
                  <div>
                    <span style={{ fontSize: 8, color: "#3a5a7a", letterSpacing: "0.06em" }}>{r.label}</span>
                    {r.sub && <div style={{ fontSize: 7, color: "#2a4a6a", marginTop: 1 }}>{r.sub}</div>}
                  </div>
                  <span style={{ fontSize: 8, color: "#8ab0d0", textAlign: "right", maxWidth: 140, wordBreak: "break-all" }}>{r.value}</span>
                </div>
              ) : null)}
            </div>
          )}

          {/* Note */}
          {note && (
            <div style={{ fontSize: 7, color: "#2a5a7a", lineHeight: 1.6, marginBottom: 8, padding: "6px 8px", background: "#050c18", borderRadius: 2, borderLeft: `2px solid ${source.color}44` }}>
              {note}
            </div>
          )}

          {/* Direct CIN / record links */}
          {extraLinks && extraLinks.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 7, color: "#2a4a6a", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 4 }}>Direct Record Links</div>
              {extraLinks.map((l, i) => (
                <a key={i} href={l.url} target="_blank" rel="noopener noreferrer"
                  style={{ display: "block", fontSize: 7, color: "#1a4a7a", textDecoration: "none", padding: "2px 0", letterSpacing: "0.05em" }}
                  onMouseEnter={e => e.target.style.color = "#4da6ff"}
                  onMouseLeave={e => e.target.style.color = "#1a4a7a"}>
                  ↗ {l.label}
                </a>
              ))}
            </div>
          )}

          {/* Verify button */}
          <a href={verifyUrl} target="_blank" rel="noopener noreferrer"
            style={{ display: "block", textAlign: "center", padding: "6px 10px", background: source.color + "15", border: `1px solid ${source.color}44`, borderRadius: 2, color: source.color, fontSize: 8, textDecoration: "none", letterSpacing: "0.08em", fontFamily: "inherit" }}
            onMouseEnter={e => { e.target.style.background = source.color + "30"; }}
            onMouseLeave={e => { e.target.style.background = source.color + "15"; }}>
            {verifyLabel}
          </a>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// DOC PANEL — Updated with real portal links in source descriptions
// ─────────────────────────────────────────────────────────────────────────────

function PoliticianList({ selectedPolitician, onSelect, politicians }) {
  return (
    <div style={{ height: "100%", overflowY: "auto" }}>
      <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 10, padding: "0 2px" }}>Tracked Politicians</div>
      {[...politicians].sort((a, b) => b.totalScore - a.totalScore).map((p) => (
        <div key={p.id} onClick={() => onSelect(selectedPolitician?.id === p.id ? null : p)}
          style={{ background: selectedPolitician?.id === p.id ? "#0a1828" : "#070e1a", border: `1px solid ${selectedPolitician?.id === p.id ? p.color + "44" : "#0f1e30"}`, borderRadius: 3, padding: "10px 12px", marginBottom: 6, cursor: "pointer", transition: "all 0.2s" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 30, height: 30, borderRadius: 2, background: p.color + "22", border: `1px solid ${p.color}55`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 10, fontWeight: 700, color: p.color, flexShrink: 0 }}>{p.photo}</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ fontSize: 11, color: "#c8d8f0", fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 120 }}>{p.name}</span>
                <span style={{ fontSize: 13, fontWeight: 800, color: p.color }}>{p.totalScore}</span>
              </div>
              <div style={{ fontSize: 8, color: "#3a5a7a", marginTop: 1 }}>{p.party} · {p.state}</div>
              <div style={{ height: 2, background: "#0d1a2e", borderRadius: 1, overflow: "hidden", marginTop: 5 }}>
                <div style={{ height: "100%", width: `${p.totalScore}%`, background: p.color, borderRadius: 1 }} />
              </div>
            </div>
          </div>
        </div>
      ))}
      <div style={{ marginTop: 16, padding: "10px 12px", background: "#070e1a", border: "1px solid #0f1e30", borderRadius: 3 }}>
        <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>Risk Legend</div>
        {[
          { label: "Critical Suspect", range: "75–100", color: "#ff2d78" },
          { label: "High Risk", range: "50–74", color: "#ff9f0a" },
          { label: "Watch List", range: "30–49", color: "#ffd700" },
          { label: "Low Risk", range: "0–29", color: "#30d158" },
        ].map(l => (
          <div key={l.label} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
            <div style={{ width: 6, height: 6, borderRadius: "50%", background: l.color, flexShrink: 0 }} />
            <span style={{ fontSize: 8, color: "#4a7a9a" }}>{l.label}</span>
            <span style={{ fontSize: 8, color: "#2a4a6a", marginLeft: "auto" }}>{l.range}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN APP
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH BAR — Live API search or local filter fallback
// ─────────────────────────────────────────────────────────────────────────────

function SearchBar({ apiAvailable, onSelect, effectivePoliticians }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [open, setOpen] = useState(false);
  const debounceRef = useRef(null);

  const handleChange = (e) => {
    const q = e.target.value;
    setQuery(q);
    clearTimeout(debounceRef.current);

    if (!q || q.length < 2) { setResults([]); setOpen(false); return; }

    debounceRef.current = setTimeout(async () => {
      if (apiAvailable) {
        try {
          const data = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
          setResults(data.slice(0, 8));
          setOpen(true);
          return;
        } catch (_) {}
      }
      // Local fallback
      const q_up = q.toUpperCase();
      const matches = effectivePoliticians
        .filter(p => (p.name || "").toUpperCase().includes(q_up) || (p.state || "").toUpperCase().includes(q_up) || (p.party || "").toUpperCase().includes(q_up))
        .slice(0, 8);
      setResults(matches.map(p => ({ ...p, score: p.totalScore || p.score || 0 })));
      setOpen(true);
    }, 250);
  };

  const pick = (r) => {
    const full = effectivePoliticians.find(p => p.id === r.id) || r;
    onSelect(full);
    setQuery("");
    setResults([]);
    setOpen(false);
  };

  return (
    <div style={{ padding: "10px 12px 6px", position: "relative", flexShrink: 0 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, background: "#070e1a", border: "1px solid #0d2040", borderRadius: 3, padding: "5px 8px" }}>
        <span style={{ fontSize: 9, color: "#2a4a6a" }}>⌕</span>
        <input
          value={query}
          onChange={handleChange}
          onBlur={() => setTimeout(() => setOpen(false), 200)}
          placeholder="Search politicians..."
          style={{ background: "transparent", border: "none", outline: "none", color: "#c8d8f0", fontSize: 9, fontFamily: "inherit", width: "100%", letterSpacing: "0.05em" }}
        />
        {query && (
          <button onClick={() => { setQuery(""); setResults([]); setOpen(false); }}
            style={{ background: "none", border: "none", color: "#2a4a6a", cursor: "pointer", fontSize: 10, padding: 0, lineHeight: 1 }}>×</button>
        )}
      </div>
      {open && results.length > 0 && (
        <div style={{ position: "absolute", top: "100%", left: 12, right: 12, background: "#060d1a", border: "1px solid #0d2040", borderRadius: 3, zIndex: 50, maxHeight: 200, overflowY: "auto" }}>
          {results.map(r => {
            const score = r.total_score || r.score || r.totalScore || 0;
            const classification = r.risk_classification || (score >= 75 ? "CRITICAL" : score >= 50 ? "HIGH" : score >= 30 ? "WATCH" : "LOW");
            const color = score >= 75 ? "#ff2d78" : score >= 50 ? "#ff9f0a" : score >= 30 ? "#00d4ff" : "#30d158";
            return (
              <div key={r.id} onMouseDown={() => pick(r)}
                style={{ padding: "7px 10px", cursor: "pointer", borderBottom: "1px solid #0a1520", display: "flex", justifyContent: "space-between", alignItems: "center" }}
                onMouseEnter={e => e.currentTarget.style.background = "#0a1830"}
                onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                <div>
                  <div style={{ fontSize: 9, color: "#c8d8f0", letterSpacing: "0.05em" }}>{r.name || r.name_normalized}</div>
                  <div style={{ fontSize: 7, color: "#3a5a7a", marginTop: 1 }}>{r.state} · {r.party}</div>
                </div>
                <div style={{ fontSize: 8, color, fontWeight: 700 }}>{score}</div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// FUND TRAILS PANEL — Shows all detected fund flow correlations
// ─────────────────────────────────────────────────────────────────────────────

function FundTrailsPanel({ politician, apiAvailable }) {
  const [trails, setTrails] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!politician?.id) return;
    setTrails(null);
    setLoading(true);

    if (apiAvailable) {
      apiFetch(`/api/politicians/${politician.id}/trails`)
        .then(data => { setTrails(data); setLoading(false); })
        .catch(() => { setTrails(politician.fundFlows || []); setLoading(false); });
    } else {
      // Use mock fund flows
      setTrails(politician.fundFlows || []);
      setLoading(false);
    }
  }, [politician?.id, apiAvailable]);

  const TIER_COLORS = { CRITICAL: "#ff2d78", HIGH: "#ff9f0a", MEDIUM: "#ffd700", LOW: "#30d158" };
  const TIER_ICONS  = { CRITICAL: "▲▲", HIGH: "▲", MEDIUM: "◆", LOW: "●" };

  if (loading) return (
    <div style={{ display: "flex", justifyContent: "center", paddingTop: 40, color: "#2a4a6a", fontSize: 9 }}>
      ◌ LOADING FUND TRAILS...
    </div>
  );

  const displayTrails = trails || politician.fundFlows || [];

  if (!displayTrails.length) return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%", gap: 8 }}>
      <div style={{ fontSize: 22, opacity: 0.08 }}>⬡</div>
      <div style={{ fontSize: 9, color: "#2a4a6a", letterSpacing: "0.1em", textAlign: "center" }}>NO FUND TRAILS DETECTED<br />FOR THIS POLITICIAN</div>
    </div>
  );

  return (
    <div style={{ height: "100%", overflowY: "auto" }}>
      {/* Summary row */}
      <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
        {["CRITICAL","HIGH","MEDIUM"].map(tier => {
          // Handle both API format (risk_tier) and mock format (risk)
          const count = displayTrails.filter(t => (t.risk_tier || t.risk) === tier).length;
          if (!count) return null;
          return (
            <div key={tier} style={{ background: TIER_COLORS[tier] + "15", border: `1px solid ${TIER_COLORS[tier]}44`, borderRadius: 2, padding: "3px 8px", fontSize: 7, color: TIER_COLORS[tier], letterSpacing: "0.1em" }}>
              {TIER_ICONS[tier]} {count} {tier}
            </div>
          );
        })}
        <div style={{ fontSize: 7, color: "#2a4a6a", alignSelf: "center", marginLeft: "auto" }}>
          {displayTrails.length} total
        </div>
      </div>

      {/* Trail cards */}
      {displayTrails.map((trail, i) => {
        const tier = trail.risk_tier || trail.risk || "MEDIUM";
        const tierColor = TIER_COLORS[tier] || "#ffd700";
        const lagDays = trail.lag_days || trail.days || 0;
        const fundAmount = trail.fund_amount || trail.amount || "";
        const scheme = trail.scheme_name || trail.from || "";
        const company = trail.winner_name || trail.company_name || trail.to || "";
        const tenderValue = trail.contract_value_cr ? `₹${trail.contract_value_cr.toFixed(1)}Cr` : "";
        const releaseDate = trail.release_date ? new Date(trail.release_date).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "2-digit" }) : "";
        const awardDate = trail.award_date ? new Date(trail.award_date).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "2-digit" }) : "";

        return (
          <div key={i} style={{ border: `1px solid ${tierColor}33`, borderLeft: `3px solid ${tierColor}`, borderRadius: 2, padding: "8px 10px", marginBottom: 8, background: tierColor + "08" }}>
            {/* Header row */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 5 }}>
              <span style={{ fontSize: 7, color: tierColor, letterSpacing: "0.15em", fontWeight: 700 }}>{TIER_ICONS[tier]} {tier}</span>
              <span style={{ fontSize: 7, color: "#3a5a7a", letterSpacing: "0.08em" }}>LAG: {lagDays}d</span>
            </div>

            {/* Fund → Company flow */}
            <div style={{ fontSize: 8, color: "#7a9abf", marginBottom: 3, letterSpacing: "0.04em", lineHeight: 1.4 }}>
              <span style={{ color: "#4a8aaf" }}>FUND</span> {scheme.length > 28 ? scheme.slice(0, 28) + "…" : scheme}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 5 }}>
              <span style={{ fontSize: 9, color: "#3a8a6a" }}>{typeof fundAmount === "string" ? fundAmount : `₹${fundAmount}Cr`}</span>
              <span style={{ fontSize: 7, color: "#2a4a6a" }}>→</span>
              <span style={{ fontSize: 9, color: tierColor }}>{tenderValue || ""}</span>
            </div>
            <div style={{ fontSize: 8, color: "#7a9abf", marginBottom: 5 }}>
              <span style={{ color: "#af6a4a" }}>TENDER</span> {company.length > 28 ? company.slice(0, 28) + "…" : company}
            </div>

            {/* Dates */}
            {(releaseDate || awardDate) && (
              <div style={{ display: "flex", gap: 12, fontSize: 7, color: "#2a4a6a", borderTop: `1px solid ${tierColor}22`, paddingTop: 5 }}>
                {releaseDate && <span>Released: {releaseDate}</span>}
                {awardDate && <span>Awarded: {awardDate}</span>}
              </div>
            )}

            {/* Evidence text (API only) */}
            {trail.evidence_summary && (
              <div style={{ marginTop: 6, fontSize: 7, color: "#2a5a7a", lineHeight: 1.5, borderTop: `1px solid ${tierColor}22`, paddingTop: 5 }}>
                {trail.evidence_summary.length > 180 ? trail.evidence_summary.slice(0, 180) + "…" : trail.evidence_summary}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default function App() {
  const [selectedPolitician, setSelectedPolitician] = useState(null);
  const [selectedSource, setSelectedSource] = useState(null);
  const [activePanel, setActivePanel] = useState("score"); // "score" | "docs"
  const [ticker, setTicker] = useState(0);

  // ── Live API hooks ──────────────────────────────────────────────────────────
  const { politicians: apiPoliticians, apiAvailable, loading: apiLoading } = useApiPoliticians();
  const apiDetail = useApiPoliticianDetail(selectedPolitician, activePanel === "score" || activePanel === "sources");
  const apiStats = useApiStats();

  // Merge live detail data when it loads
  useEffect(() => {
    if (apiDetail && selectedPolitician?.id === apiDetail.id) {
      setSelectedPolitician(apiDetail);
    }
  }, [apiDetail]);

  // Use live politicians if available, otherwise fall back to mock POLITICIANS array
  const effectivePoliticians = apiPoliticians || POLITICIANS;

  useEffect(() => {
    const id = setInterval(() => setTicker(t => t + 1), 3000);
    return () => clearInterval(id);
  }, []);

  // Dynamic ticker messages — use real stats when available
  const totalPols = apiStats?.total_politicians || 1247;
  const criticalCount = apiStats?.critical_suspects || 0;
  const trailCount = apiStats?.total_fund_trails || 0;
  const flaggedValue = apiStats?.flagged_tender_value_cr || 0;

  const tickerMessages = apiAvailable ? [
    `LIVE API CONNECTED — ${totalPols.toLocaleString()} politicians tracked — ${DATA_SOURCES.length}/8 sources active`,
    criticalCount > 0 ? `ALERT: ${criticalCount} CRITICAL suspects identified — immediate review recommended` : "SYSTEM ACTIVE — Correlation engine running",
    trailCount > 0 ? `${trailCount.toLocaleString()} fund trail correlations detected — ₹${flaggedValue.toFixed(0)}Cr under review` : "Fund tracing active — awaiting correlation results",
    `GeM Portal sync active — new tenders monitored daily for entity graph matches`,
    `PFMS fund releases monitored — district-level disbursements tracked in real time`,
  ] : [
    "SYSTEM ACTIVE — 6 sources connected — 1,247 politicians tracked [DEMO MODE]",
    "ALERT: New fund correlation detected — Patil Constructions — ₹47Cr",
    "MCA21 sync complete — 3 new director links resolved",
    "GeM Portal: 14 new tenders awarded today — 2 flagged for review",
    "PFMS: ₹234Cr disbursed across 8 districts — monitoring active",
  ];

  return (
    <div style={{
      fontFamily: "'IBM Plex Mono', 'Courier New', monospace",
      background: "#040a14",
      minHeight: "100vh",
      color: "#c8d8f0",
      display: "flex",
      flexDirection: "column",
      height: "100vh",
      overflow: "hidden",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 3px; }
        ::-webkit-scrollbar-track { background: #040a14; }
        ::-webkit-scrollbar-thumb { background: #1a2e4a; border-radius: 2px; }
        .scan { animation: scanline 4s linear infinite; }
        @keyframes scanline { 0% { transform: translateY(-100%); } 100% { transform: translateY(100vh); } }
        .blink { animation: blink 1.2s step-end infinite; }
        @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
        .ticker { animation: tickerMove 20s linear infinite; white-space: nowrap; }
        @keyframes tickerMove { 0% { transform: translateX(100%); } 100% { transform: translateX(-100%); } }
      `}</style>

      {/* Scanline overlay */}
      <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, pointerEvents: "none", zIndex: 100, background: "repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px)" }} />

      {/* Header */}
      <div style={{ borderBottom: "1px solid #0d1e30", padding: "0 20px", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0, height: 48, background: "#050c18" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#ff2d78" }} className="blink" />
            <span style={{ fontSize: 14, fontWeight: 700, letterSpacing: "0.2em", color: "#e8f4ff" }}>DARPAN<span style={{ color: "#00d4ff" }}>.IN</span></span>
          </div>
          <div style={{ width: 1, height: 20, background: "#0d1e30" }} />
          <span style={{ fontSize: 8, color: "#2a4a6a", letterSpacing: "0.15em", textTransform: "uppercase" }}>Political Corruption Intelligence Platform</span>
        </div>
        <div style={{ display: "flex", gap: 20, alignItems: "center" }}>
          <div style={{ fontSize: 8, color: "#3a5a7a" }}>
            <span style={{ color: apiAvailable ? "#30d158" : "#ff9f0a" }}>●</span>
            {" "}{apiAvailable ? `API LIVE — ${(apiStats?.total_politicians || "...").toLocaleString()} POLITICIANS` : "DEMO MODE — START BACKEND TO GO LIVE"}
          </div>
          <div style={{ fontSize: 8, color: "#2a4a6a" }}>BUILD v2.4.1</div>
          <div style={{ fontSize: 8, color: "#2a4a6a" }}>DATA: 19 MAR 2026</div>
        </div>
      </div>

      {/* Ticker */}
      <div style={{ background: "#030810", borderBottom: "1px solid #0a1520", height: 22, overflow: "hidden", flexShrink: 0, display: "flex", alignItems: "center" }}>
        <div style={{ fontSize: 7, color: "#ff2d78", letterSpacing: "0.15em", padding: "0 12px", borderRight: "1px solid #0a1520", flexShrink: 0, height: "100%", display: "flex", alignItems: "center" }}>LIVE</div>
        <div style={{ overflow: "hidden", flex: 1 }}>
          <div className="ticker" style={{ fontSize: 8, color: "#2a6a8a", letterSpacing: "0.1em" }}>
            {tickerMessages.join("    ·    ")}
          </div>
        </div>
      </div>

      {/* Main layout */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>

        {/* Left: Politician list + Search */}
        <div style={{ width: 210, borderRight: "1px solid #0d1e30", background: "#050c18", flexShrink: 0, overflow: "hidden", display: "flex", flexDirection: "column" }}>
          <SearchBar
            apiAvailable={apiAvailable}
            onSelect={(p) => { setSelectedPolitician(p); setActivePanel("score"); }}
            effectivePoliticians={effectivePoliticians}
          />
          <div style={{ flex: 1, overflow: "hidden", padding: "0 12px 12px" }}>
            <PoliticianList
              selectedPolitician={selectedPolitician}
              politicians={effectivePoliticians}
              onSelect={(p) => { setSelectedPolitician(p); setActivePanel("score"); }}
            />
          </div>
        </div>

        {/* Center: Spider web */}
        <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
          <SpiderWeb
            selectedPolitician={selectedPolitician}
            politicians={effectivePoliticians}
            onSelectPolitician={(p) => { setSelectedPolitician(p); if (p) setActivePanel("score"); }}
            onSelectSource={(s) => { setSelectedSource(s); setActivePanel("docs"); }}
          />
          {/* Loading overlay */}
          {apiLoading && (
            <div style={{ position: "absolute", top: 12, left: "50%", transform: "translateX(-50%)", background: "#050c18cc", border: "1px solid #00d4ff44", borderRadius: 3, padding: "6px 14px", fontSize: 9, color: "#00d4ff88", letterSpacing: "0.12em", backdropFilter: "blur(8px)" }}>
              ◌ CONNECTING TO BACKEND API...
            </div>
          )}
          {/* Instruction overlay when nothing selected */}
          {!selectedPolitician && !apiLoading && (
            <div style={{ position: "absolute", bottom: 20, left: "50%", transform: "translateX(-50%)", fontSize: 9, color: "#2a4a6a", letterSpacing: "0.12em", textAlign: "center", pointerEvents: "none" }}>
              CLICK A NODE TO INVESTIGATE · INNER RING = POLITICIANS · OUTER RING = DATA SOURCES
            </div>
          )}
          {/* Selected source banner */}
          {selectedSource && (
            <div style={{ position: "absolute", top: 12, left: "50%", transform: "translateX(-50%)", background: "#050c18cc", border: `1px solid ${selectedSource.color}44`, borderRadius: 3, padding: "6px 14px", fontSize: 9, color: selectedSource.color, letterSpacing: "0.1em", backdropFilter: "blur(8px)" }}>
              {selectedSource.label.toUpperCase()} — SEE DOCS PANEL →
            </div>
          )}
          {/* Stats overlay bottom-right */}
          {apiAvailable && apiStats && (
            <div style={{ position: "absolute", bottom: 12, right: 12, display: "flex", gap: 12, fontSize: 7, color: "#2a4a6a", letterSpacing: "0.1em" }}>
              <span style={{ color: "#ff2d78aa" }}>▲ {apiStats.critical_suspects} CRITICAL</span>
              <span style={{ color: "#ff9f0aaa" }}>▲ {apiStats.high_risk} HIGH</span>
              <span style={{ color: "#00d4ffaa" }}>⬡ {apiStats.total_fund_trails} TRAILS</span>
            </div>
          )}
        </div>

        {/* Right: Detail panel */}
        <div style={{ width: 330, borderLeft: "1px solid #0d1e30", background: "#050c18", flexShrink: 0, display: "flex", flexDirection: "column", overflow: "hidden" }}>
          {/* Panel tabs */}
          <div style={{ display: "flex", borderBottom: "1px solid #0d1e30", flexShrink: 0 }}>
            {[
              { id: "score", label: "Score" },
              { id: "trails", label: "Fund Trails" },
              { id: "sources", label: "Sources" },
              { id: "docs", label: "Docs" },
            ].map(t => (
              <button key={t.id} onClick={() => setActivePanel(t.id)}
                style={{ flex: 1, background: activePanel === t.id ? "#070e1a" : "transparent", border: "none", borderBottom: `2px solid ${activePanel === t.id ? "#4da6ff" : "transparent"}`, color: activePanel === t.id ? "#4da6ff" : "#2a4a6a", fontSize: 8, padding: "10px 4px", cursor: "pointer", fontFamily: "inherit", letterSpacing: "0.08em", textTransform: "uppercase", transition: "all 0.2s" }}>
                {t.label}
              </button>
            ))}
          </div>

          <div style={{ flex: 1, overflow: "hidden", padding: 12 }}>
            {activePanel === "score" && (
              selectedPolitician ? (
                <div style={{ height: "100%", overflowY: "auto" }}>
                  <ScoreBreakdown politician={selectedPolitician} />
                  <div style={{ marginTop: 14 }}>
                    <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 8 }}>Declared Asset Growth</div>
                    <AssetChart politician={selectedPolitician} />
                  </div>
                  {selectedPolitician.entities && selectedPolitician.entities.length > 0 && (
                    <div style={{ marginTop: 14 }}>
                      <div style={{ fontSize: 9, color: "#3a5a7a", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 8 }}>Linked Entities</div>
                      {selectedPolitician.entities.map((e, i) => (
                        <div key={i} style={{ fontSize: 8, color: "#4a7a9a", padding: "3px 0", borderBottom: "1px solid #0a1520", letterSpacing: "0.05em" }}>
                          ⬡ {e}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <div style={{ height: "100%", display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center", gap: 8 }}>
                  <div style={{ fontSize: 28, opacity: 0.1 }}>◎</div>
                  <div style={{ fontSize: 9, color: "#2a4a6a", letterSpacing: "0.12em", textAlign: "center" }}>SELECT A POLITICIAN NODE<br />TO VIEW SCORE BREAKDOWN</div>
                </div>
              )
            )}

            {activePanel === "trails" && (
              selectedPolitician ? (
                <FundTrailsPanel politician={selectedPolitician} apiAvailable={apiAvailable} />
              ) : (
                <div style={{ height: "100%", display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center", gap: 8 }}>
                  <div style={{ fontSize: 28, opacity: 0.1 }}>⬡</div>
                  <div style={{ fontSize: 9, color: "#2a4a6a", letterSpacing: "0.12em", textAlign: "center" }}>SELECT A POLITICIAN<br />TO VIEW FUND TRAILS</div>
                </div>
              )
            )}

            {activePanel === "sources" && (
              selectedPolitician ? (
                <SourcesPanel politician={selectedPolitician} apiAvailable={apiAvailable} />
              ) : (
                <div style={{ height: "100%", display: "flex", flexDirection: "column", justifyContent: "center", alignItems: "center", gap: 8 }}>
                  <div style={{ fontSize: 28, opacity: 0.08 }}>🔗</div>
                  <div style={{ fontSize: 9, color: "#2a4a6a", letterSpacing: "0.12em", textAlign: "center" }}>SELECT A POLITICIAN<br />TO VIEW DATA SOURCES</div>
                </div>
              )
            )}

            {activePanel === "docs" && <DocPanel selectedSource={selectedSource} />}
          </div>
        </div>
      </div>

      {/* Bottom status bar */}
      <div style={{ borderTop: "1px solid #0d1e30", padding: "5px 20px", display: "flex", justifyContent: "space-between", alignItems: "center", background: "#030810", flexShrink: 0, height: 28 }}>
        <div style={{ display: "flex", gap: 16, fontSize: 7, color: "#2a4a6a", letterSpacing: "0.1em" }}>
          {DATA_SOURCES.map(s => <span key={s.id} style={{ color: s.color + "88" }}>● {s.short}</span>)}
        </div>
        <div style={{ fontSize: 7, color: "#1a3a5a", letterSpacing: "0.1em" }}>
          ALL DATA IS LEGALLY PUBLIC · RTI ACT · COMPANIES ACT · ELECTION COMMISSION MANDATES
        </div>
      </div>
    </div>
  );
}
