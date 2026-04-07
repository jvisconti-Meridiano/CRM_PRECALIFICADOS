import {h} from "./deps.js";

const STORAGE_KEY = "meridiano-theme";
const STYLE_ID = "meridiano-theme-runtime";

const THEMES = {
  dark: {
    key: "dark",
    label: "Oscuro",
    bg: {
      base: "#07110F",
      shell: "rgba(6,18,16,0.86)",
      shell2: "rgba(9,26,23,0.92)",
      surface: "rgba(10,21,19,0.82)",
      surface2: "rgba(12,28,24,0.92)",
      card: "linear-gradient(180deg, rgba(18,32,29,0.92) 0%, rgba(11,21,20,0.94) 100%)",
      cardSoft: "rgba(255,255,255,0.045)",
      hover: "rgba(255,255,255,0.06)",
      input: "rgba(0,0,0,0.24)",
      inputFocus: "rgba(56,189,160,0.14)",
      accent: "linear-gradient(135deg, rgba(56,189,160,0.18) 0%, rgba(46,102,240,0.12) 100%)",
      panel: "rgba(9,24,21,0.84)",
      tableHead: "rgba(9,22,20,0.96)",
      overlay: "rgba(3,10,8,0.7)",
    },
    bd: {
      s: "rgba(159,232,112,0.10)",
      d: "rgba(178,242,228,0.16)",
      strong: "rgba(178,242,228,0.24)",
      focus: "rgba(56,189,160,0.42)",
    },
    t: {
      p: "#F4FFF9",
      s: "#C7ECDD",
      m: "#7EA095",
      a: "#9AD4FF",
      ok: "#A7F37A",
      err: "#FF9A9A",
      w: "#FFD36E",
      inv: "#08110F",
    },
    c: {
      blue: "#2E66F0",
      blue2: "#5C86FF",
      blueG: "rgba(46,102,240,0.16)",
      green: "#00A97D",
      green2: "#38BDA0",
      greenG: "rgba(0,169,125,0.18)",
      red: "#EF4444",
      redG: "rgba(239,68,68,0.12)",
      amber: "#F59E0B",
      amberG: "rgba(245,158,11,0.12)",
      lime: "#5CC21A",
      teal: "#38BDA0",
      emerald: "#00A97D",
    },
    fx: {
      glowA: "radial-gradient(circle at 20% 20%, rgba(56,189,160,0.18), transparent 52%)",
      glowB: "radial-gradient(circle at 82% 10%, rgba(46,102,240,0.16), transparent 40%)",
      glowC: "radial-gradient(circle at 50% 120%, rgba(92,194,26,0.08), transparent 34%)",
      grid: "rgba(178,242,228,0.032)",
    },
    r: { sm: "10px", md: "14px", lg: "22px", pill: "999px" },
    f: { s: "'DM Sans',system-ui,sans-serif", m: "'JetBrains Mono',monospace" },
    shadow: {
      card: "0 22px 60px rgba(0,0,0,0.34)",
      pop: "0 28px 90px rgba(0,0,0,0.48)",
      glow: "0 0 0 1px rgba(178,242,228,0.08), 0 18px 48px rgba(0,0,0,0.34)",
      inset: "inset 0 1px 0 rgba(255,255,255,0.04)",
    },
  },
  light: {
    key: "light",
    label: "Claro",
    bg: {
      base: "#F4FBF8",
      shell: "rgba(255,255,255,0.76)",
      shell2: "rgba(244,251,248,0.92)",
      surface: "rgba(255,255,255,0.78)",
      surface2: "rgba(255,255,255,0.92)",
      card: "linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(248,252,250,0.98) 100%)",
      cardSoft: "rgba(255,255,255,0.86)",
      hover: "rgba(46,102,240,0.06)",
      input: "rgba(7,74,53,0.045)",
      inputFocus: "rgba(46,102,240,0.07)",
      accent: "linear-gradient(135deg, rgba(56,189,160,0.16) 0%, rgba(46,102,240,0.10) 100%)",
      panel: "rgba(255,255,255,0.82)",
      tableHead: "rgba(248,252,250,0.98)",
      overlay: "rgba(239,247,244,0.62)",
    },
    bd: {
      s: "rgba(7,74,53,0.08)",
      d: "rgba(7,74,53,0.14)",
      strong: "rgba(7,74,53,0.22)",
      focus: "rgba(46,102,240,0.24)",
    },
    t: {
      p: "#102A22",
      s: "#45675B",
      m: "#789087",
      a: "#2E66F0",
      ok: "#0B8A57",
      err: "#C53D3D",
      w: "#A96C00",
      inv: "#FFFFFF",
    },
    c: {
      blue: "#2E66F0",
      blue2: "#5C86FF",
      blueG: "rgba(46,102,240,0.12)",
      green: "#00A97D",
      green2: "#38BDA0",
      greenG: "rgba(0,169,125,0.12)",
      red: "#EF4444",
      redG: "rgba(239,68,68,0.08)",
      amber: "#F59E0B",
      amberG: "rgba(245,158,11,0.10)",
      lime: "#5CC21A",
      teal: "#38BDA0",
      emerald: "#00A97D",
    },
    fx: {
      glowA: "radial-gradient(circle at 12% 12%, rgba(56,189,160,0.18), transparent 42%)",
      glowB: "radial-gradient(circle at 88% 8%, rgba(46,102,240,0.12), transparent 36%)",
      glowC: "radial-gradient(circle at 50% 100%, rgba(92,194,26,0.07), transparent 28%)",
      grid: "rgba(7,74,53,0.028)",
    },
    r: { sm: "10px", md: "14px", lg: "22px", pill: "999px" },
    f: { s: "'DM Sans',system-ui,sans-serif", m: "'JetBrains Mono',monospace" },
    shadow: {
      card: "0 24px 70px rgba(11,27,22,0.09)",
      pop: "0 28px 90px rgba(11,27,22,0.14)",
      glow: "0 0 0 1px rgba(7,74,53,0.04), 0 22px 60px rgba(11,27,22,0.08)",
      inset: "inset 0 1px 0 rgba(255,255,255,0.66)",
    },
  },
};

const _clone = obj => JSON.parse(JSON.stringify(obj));
const T = _clone(THEMES.dark);

function getStoredTheme() {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    return THEMES[v] ? v : "dark";
  } catch (_) {
    return "dark";
  }
}

function ensureRuntimeStyle() {
  if (typeof document === "undefined" || document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
    html,body,#root{min-height:100%;}
    body{
      position:relative;
      overflow:hidden;
      background:var(--bg-base);
      color:var(--text-primary);
    }
    body::before,
    body::after{
      content:"";
      position:fixed;
      inset:0;
      pointer-events:none;
      z-index:0;
      transition:opacity .45s ease, background .45s ease, filter .45s ease;
    }
    body::before{
      background:var(--fx-glow-a), var(--fx-glow-b), var(--fx-glow-c);
      filter:saturate(110%);
      opacity:1;
    }
    body::after{
      background-image:
        linear-gradient(var(--fx-grid) 1px, transparent 1px),
        linear-gradient(90deg, var(--fx-grid) 1px, transparent 1px);
      background-size:32px 32px;
      mask-image:linear-gradient(180deg, rgba(0,0,0,.18), rgba(0,0,0,0));
      opacity:.55;
    }
    #root{
      position:relative;
      z-index:1;
      isolation:isolate;
    }
    #root, #root *{
      transition:
        background .32s cubic-bezier(.22,1,.36,1),
        background-color .32s cubic-bezier(.22,1,.36,1),
        color .26s cubic-bezier(.22,1,.36,1),
        border-color .32s cubic-bezier(.22,1,.36,1),
        box-shadow .36s cubic-bezier(.22,1,.36,1),
        opacity .24s ease,
        transform .22s ease;
    }
    input::placeholder, textarea::placeholder{color:var(--text-muted);}
    ::selection{background:var(--color-blue-glow);color:var(--text-primary);}
  `;
  document.head.appendChild(style);
}

function applyVars(theme) {
  if (typeof document === "undefined") return;
  const root = document.documentElement;
  const vars = {
    "--bg-base": theme.bg.base,
    "--bg-shell": theme.bg.shell,
    "--bg-shell-2": theme.bg.shell2,
    "--bg-surface": theme.bg.surface,
    "--bg-surface-2": theme.bg.surface2,
    "--bg-card": theme.bg.cardSoft,
    "--bg-input": theme.bg.input,
    "--bg-accent": theme.bg.accent,
    "--bg-panel": theme.bg.panel,
    "--text-primary": theme.t.p,
    "--text-secondary": theme.t.s,
    "--text-muted": theme.t.m,
    "--border-soft": theme.bd.s,
    "--border-strong": theme.bd.d,
    "--shadow-card": theme.shadow.card,
    "--shadow-pop": theme.shadow.pop,
    "--color-blue": theme.c.blue,
    "--color-blue-glow": theme.c.blueG,
    "--color-green": theme.c.green,
    "--color-green-glow": theme.c.greenG,
    "--fx-glow-a": theme.fx.glowA,
    "--fx-glow-b": theme.fx.glowB,
    "--fx-glow-c": theme.fx.glowC,
    "--fx-grid": theme.fx.grid,
  };
  Object.entries(vars).forEach(([k, v]) => root.style.setProperty(k, v));
}

function applyTheme(name) {
  const next = THEMES[name] ? name : "dark";
  const theme = _clone(THEMES[next]);
  Object.keys(T).forEach(k => delete T[k]);
  Object.assign(T, theme);
  try {
    localStorage.setItem(STORAGE_KEY, next);
  } catch (_) {}
  if (typeof document !== "undefined") {
    ensureRuntimeStyle();
    applyVars(T);
    document.documentElement.dataset.theme = next;
    document.documentElement.style.colorScheme = next === "light" ? "light" : "dark";
    document.documentElement.style.background = T.bg.base;
    if (document.body) {
      document.body.style.background = T.bg.base;
      document.body.style.color = T.t.p;
    }
  }
  return next;
}

function ThemeToggle({themeName="dark", onChange, compact=false}) {
  const isLight = themeName === "light";
  return h`<button
    type="button"
    onClick=${() => onChange?.(isLight ? "dark" : "light")}
    title=${isLight ? "Cambiar a tema oscuro" : "Cambiar a tema claro"}
    style=${{
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: compact ? "8px" : "12px",
      minWidth: compact ? "132px" : "148px",
      padding: compact ? "7px 10px" : "9px 12px",
      borderRadius: T.r.pill,
      border: "1px solid " + T.bd.d,
      background: T.bg.accent,
      color: T.t.p,
      cursor: "pointer",
      fontSize: compact ? "12px" : "13px",
      fontWeight: 700,
      fontFamily: T.f.s,
      boxShadow: T.shadow.glow,
      backdropFilter: "blur(14px)",
    }}
  >
    <span style=${{display:"inline-flex",alignItems:"center",gap:"8px"}}>
      <span style=${{display:"inline-grid",placeItems:"center",width:compact?"20px":"24px",height:compact?"20px":"24px",borderRadius:T.r.pill,background:T.bg.input,border:"1px solid "+T.bd.s}}>${isLight ? "☀" : "☾"}</span>
      <span>${isLight ? "Modo claro" : "Modo oscuro"}</span>
    </span>
    <span style=${{fontSize:"11px",color:T.t.m}}>${isLight ? "ON" : "OFF"}</span>
  </button>`;
}

export {T, THEMES, getStoredTheme, applyTheme, ThemeToggle};
