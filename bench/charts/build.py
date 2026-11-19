"""Benchmark charts for the docs, drawn from the raw results in bench/results/.

    uv run --no-project --with fonttools --with brotli python bench/charts/build.py

It reads every `bench/results/*.jsonl`, picks the newest run of each kind (rules below), and
writes static SVG files to `docs/assets/charts/`: every chart in a light and a dark version, each
in a wide layout and a narrow one for phones. It also rewrites the marked regions of
docs/benchmarks.md, docs/index.md and README.md (`<!-- chart:NAME -->` for the figure,
`<!-- chart-data:NAME -->` for its table of exact numbers), so the pages and the charts never
disagree. The prose around them is written by hand. Running it twice on the same results changes
no bytes.

How runs are picked:
- Tracker workloads (records with an `engine`, a `workload` and a `rep`): a comparison run is a
  file with at least two of moraine, Lance and Delta Lake. moraine's value for a measure is always
  its current default configuration: the newest comparison run that measured it, skipping what
  NOT_DEFAULT lists. Runs of moraine alone (on a loaded host) count only for peak memory and
  size, which do not depend on the load. Lance and Delta Lake come from moraine's run where it
  has them, otherwise from the newest comparison run that does. A measure with no default-path
  value for moraine is left out rather than filled with a superseded one. Each value is the median
  of the run's repetitions that passed, and the data tables say which run every value came from.
- Multi-writer scaling: the newest comparison run with moraine at three or more writer counts.
- Vector, full-text and blob runs: per data set and layout, the newest file that has it.
- SUPERSEDED lists files kept as history that no chart should read.

Colour (the dataviz method: emphasis, then a validated palette). moraine is the brand's
ochre-600 in both themes. Lance and Delta Lake are two steps of slate, the same hue at two
lightnesses: grey stone read as no colour at all (OKLCH chroma 0.02) and failed the palette
validator's chroma floor, so the stones carry just enough blue (C 0.105) to stay identifiable,
and stay cool against the warm accent. Validated with the dataviz skill's validate_palette.js,
`--pairs all`, against the docs' grounds:
    light, surface #f7f5ef: "#a87010,#5890ca,#1b558a"  all checks pass
    dark,  surface #15110a: "#a87010,#5e97d0,#2e669c"  all checks pass
Text uses the docs' ink tokens, never a series colour. Engines are named by direct labels and a
legend as well as colour. On the headline chart a measure moraine trails is an outlined bar of the
same ochre, not a status colour, with a key above the chart and the loss spelled out in its label.

The labels use Instrument Sans, the docs' text face, subset to each chart's characters and
embedded, since an SVG shown as an image cannot load the site's fonts.
"""

from __future__ import annotations

import base64
import html
import io
import json
import math
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from fontTools import subset
from fontTools.ttLib import TTFont

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "bench" / "results"
OUT = ROOT / "docs" / "assets" / "charts"
FONT = ROOT / "docs" / "assets" / "fonts" / "instrument-sans-latin-var.woff2"
REPO = "https://github.com/sayef/moraine/blob/main/"

# Files kept as history that no chart reads.
SUPERSEDED = {
    # The first rerun of the vector benchmark searched a file's parts on threads whatever the
    # fan-out; 2026-11-18-rerun-vector.jsonl is the same run with the final rule (docs/BENCH.md).
    "2026-11-18-rerun-vector-first.jsonl",
    # Before-and-after runs of the threaded builds and the search pool (D80, D83), measured
    # without Lance on a shared host; docs/BENCH.md has them next to the loads they ran at.
    "2026-11-19-vector-builds-one-thread.jsonl",
    "2026-11-19-vector-builds-threaded-pool.jsonl",
    "2026-11-19-vector-builds-threaded-per-file.jsonl",
}

# moraine records that measured a configuration which is no longer the default:
# (file, workload, label) -> what it measured. "*" matches any workload or label.
NOT_DEFAULT = {
    ("2026-11-18-rerun-local.jsonl", "optimize", None): "the in-memory merge",
    ("2026-11-18-rerun-local.jsonl", "disk", "size_after_optimize"): "the in-memory merge",
    ("2026-11-18-local-compaction-memory.jsonl", "*", "*"): "the in-memory merge",
}

# Measures that do not depend on the host's load, so moraine-only runs on a loaded host count.
LOAD_INDEPENDENT = {"peak_rss_mb", "size_mb"}

ENGINES = ("moraine", "lance", "delta")
NAMES = {"moraine": "moraine", "lance": "Lance", "delta": "Delta Lake"}

THEMES = {
    "light": {
        "surface": "#f7f5ef",
        "band": "#ede9df",
        "ink": "#1f1709",
        "ink2": "#4f4535",
        "muted": "#675c4a",
        "grid": "#dcd5c6",
        "axis": "#675c4a",
        "moraine": "#a87010",
        "lance": "#5890ca",
        "delta": "#1b558a",
    },
    "dark": {
        "surface": "#15110a",
        "band": "#201a10",
        "ink": "#f6f0e4",
        "ink2": "#cbbfa8",
        "muted": "#a69881",
        "grid": "#3e3424",
        "axis": "#a69881",
        "moraine": "#a87010",
        "lance": "#5e97d0",
        "delta": "#2e669c",
    },
}

WIDE, NARROW = 760, 400
PAD = 16
FS = 14  # label size
FS_SMALL = 13
FS_TITLE = 15


# ---- numbers ------------------------------------------------------------------------------


def fmt(v: float) -> str:
    """Four significant figures, the precision of the result files, with thousands separators."""
    if v == 0:
        return "0"
    a = abs(v)
    if a >= 1000:
        return f"{v:,.0f}"
    decimals = max(0, 3 - math.floor(math.log10(a)))
    s = f"{v:.{min(decimals, 3)}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def fmt_ratio(r: float) -> str:
    if r >= 10:
        return f"{r:,.0f}"
    if r >= 2:
        return f"{r:.1f}"
    return f"{r:.2f}"


def nice_max(v: float) -> float:
    exp = math.floor(math.log10(v))
    for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * 10**exp >= v:
            return m * 10**exp
    return 10 ** (exp + 1)


def linear_ticks(vmax: float, n: int = 4) -> list[float]:
    raw = vmax / n
    exp = math.floor(math.log10(raw))
    step = min(m * 10**exp for m in (1, 2, 2.5, 5, 10) if m * 10**exp >= raw)
    ticks, t = [], 0.0
    while t <= vmax * 1.0001:
        ticks.append(round(t, 10))
        t += step
    return ticks


# ---- results ------------------------------------------------------------------------------


@dataclass
class Run:
    name: str
    records: list[dict]

    @property
    def date(self) -> str:
        return self.name[:10]


def load_runs() -> list[Run]:
    runs = []
    for path in sorted(RESULTS.glob("*.jsonl")):
        if path.name in SUPERSEDED:
            continue
        records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        runs.append(Run(path.name, records))
    return runs


def day(name: str) -> str:
    """'2026-11-17-fair-local.jsonl' -> '17 Nov'."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    _, m, d = name[:10].split("-")
    return f"{int(d)} {months[int(m) - 1]}"


@dataclass
class Stat:
    median: float | None
    lo: float | None
    hi: float | None
    ok: int
    failed: int
    source: str

    def cell(self, unit: str) -> str:
        if self.median is None:
            return f"failed in {self.failed} of {self.failed} runs"
        s = f"{fmt(self.median)} {unit}".strip()
        if self.ok > 1 and self.lo != self.hi:
            s += f" ({fmt(self.lo)}–{fmt(self.hi)})"
        if self.failed:
            s += f", failed in {self.failed} of {self.ok + self.failed} runs"
        return s


def stat_of(values: list[float], failed: int, source: str) -> Stat:
    if not values:
        return Stat(None, None, None, 0, failed, source)
    return Stat(statistics.median(values), min(values), max(values), len(values), failed, source)


class Tracker:
    """The tracker workloads of the comparison runs."""

    def __init__(self, runs: list[Run]):
        self.runs = []  # comparison runs
        self.solo = []  # moraine alone
        for run in runs:
            recs = [
                r
                for r in run.records
                if "workload" in r and "rep" in r and not r.get("kv0_maintenance")
            ]
            engines = {r["engine"] for r in recs} & set(ENGINES)
            if len(engines) >= 2:
                self.runs.append(Run(run.name, recs))
            elif engines == {"moraine"}:
                self.solo.append(Run(run.name, recs))

    @staticmethod
    def not_default(run, workload, label) -> str | None:
        for key in ((run.name, workload, label), (run.name, workload, "*"), (run.name, "*", "*")):
            if key in NOT_DEFAULT:
                return NOT_DEFAULT[key]
        return None

    @staticmethod
    def _rows(run, engine, backend, workload, label):
        return [
            r
            for r in run.records
            if r["engine"] == engine
            and r["backend"] == backend
            and r["workload"] == workload
            and r.get("label") == label
        ]

    def _has(self, run, engine, backend, workload, label, metric) -> bool:
        return any(
            r["status"] == "ok" and metric in r["metrics"]
            for r in self._rows(run, engine, backend, workload, label)
        ) or any(r["status"] == "error" for r in self._rows(run, engine, backend, workload, label))

    def _stat(self, run, engine, backend, workload, label, metric) -> Stat:
        rows = self._rows(run, engine, backend, workload, label)
        ok = [r["metrics"][metric] for r in rows if r["status"] == "ok" and metric in r["metrics"]]
        failed = sum(1 for r in rows if r["status"] == "error")
        return stat_of(ok, failed, run.name)

    def measure(self, backend, workload, label, metric) -> dict[str, Stat]:
        """moraine's current default, and the rivals from its run or the newest that has them."""
        pool = self.runs + (self.solo if metric in LOAD_INDEPENDENT else [])
        mine = [
            run
            for run in pool
            if self._has(run, "moraine", backend, workload, label, metric)
            and not self.not_default(run, workload, label)
        ]
        out = {}
        home = max(mine, key=lambda run: run.name) if mine else None
        if home:
            out["moraine"] = self._stat(home, "moraine", backend, workload, label, metric)
        order = ([home] if home in self.runs else []) + sorted(
            self.runs, key=lambda run: run.name, reverse=True
        )
        for e in ENGINES[1:]:
            for run in order:
                if self._has(run, e, backend, workload, label, metric):
                    out[e] = self._stat(run, e, backend, workload, label, metric)
                    break
        return out

    def superseded(self, backend, workload, label, metric) -> list[tuple[str, Stat]]:
        """moraine's figures from configurations that are no longer the default, newest first."""
        out = []
        for run in sorted(self.runs + self.solo, key=lambda run: run.name, reverse=True):
            why = self.not_default(run, workload, label)
            if why and self._has(run, "moraine", backend, workload, label, metric):
                out.append((why, self._stat(run, "moraine", backend, workload, label, metric)))
        return out

    def scaling_run(self, backend) -> Run | None:
        def counts(run):
            return {
                r.get("label")
                for r in run.records
                if r["engine"] == "moraine"
                and r["backend"] == backend
                and r["workload"] == "multi_writer"
            }

        runs = [run for run in self.runs if len(counts(run)) >= 3]
        return max(runs, key=lambda run: run.name) if runs else None


# ---- text metrics and fonts -----------------------------------------------------------------

_FONT_BYTES = FONT.read_bytes()
_METRIC_FONT = TTFont(io.BytesIO(_FONT_BYTES))
_CMAP = _METRIC_FONT.getBestCmap()
_HMTX = _METRIC_FONT["hmtx"]
_UPM = _METRIC_FONT["head"].unitsPerEm


def text_width(s: str, size: float = FS, weight: int = 400) -> float:
    total = 0
    for ch in s:
        glyph = _CMAP.get(ord(ch))
        total += _HMTX[glyph][0] if glyph else 550
    return total / _UPM * size * (1.045 if weight >= 600 else 1.0)


def font_face(chars: str) -> str:
    # recalcTimestamp=False keeps head.modified from the source font, so the output is stable
    font = TTFont(io.BytesIO(_FONT_BYTES), recalcTimestamp=False)
    options = subset.Options()
    options.flavor = "woff2"
    options.layout_features = ["*"]
    options.name_IDs = []
    options.notdef_outline = True
    sub = subset.Subsetter(options)
    sub.populate(text="".join(sorted(set(chars + "0123456789"))))
    sub.subset(font)
    buf = io.BytesIO()
    font.flavor = "woff2"
    font.save(buf, reorderTables=True)
    data = base64.b64encode(buf.getvalue()).decode()
    return (
        '@font-face{font-family:"Instrument Sans";font-weight:400 700;'
        f'src:url(data:font/woff2;base64,{data}) format("woff2")}}'
    )


# ---- SVG ----------------------------------------------------------------------------------


def n(v: float) -> str:
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return "0" if s == "-0" else s


class Svg:
    def __init__(self, width: int, theme: str):
        self.w = width
        self.t = THEMES[theme]
        self.parts: list[str] = []
        self.chars: list[str] = []

    def color(self, c: str) -> str:
        return self.t.get(c, c)

    def text(self, x, y, s, size=FS, weight=400, fill="ink2", anchor="start"):
        self.chars.append(s)
        attrs = f'x="{n(x)}" y="{n(y)}" font-size="{size}" fill="{self.color(fill)}"'
        if weight != 400:
            attrs += f' font-weight="{weight}"'
        if anchor != "start":
            attrs += f' text-anchor="{anchor}"'
        self.parts.append(f"<text {attrs}>{html.escape(s)}</text>")

    def line(self, x1, y1, x2, y2, stroke="grid", width=1):
        self.parts.append(
            f'<line x1="{n(x1)}" y1="{n(y1)}" x2="{n(x2)}" y2="{n(y2)}" '
            f'stroke="{self.color(stroke)}" stroke-width="{width}"/>'
        )

    def rect(self, x, y, w, h, fill, rx=0):
        r = f' rx="{rx}"' if rx else ""
        self.parts.append(
            f'<rect x="{n(x)}" y="{n(y)}" width="{n(w)}" height="{n(h)}"{r} fill="{self.color(fill)}"/>'
        )

    def bar(self, x0, x1, y, h, fill, title, hit=None, outline=False):
        """A horizontal bar from baseline x0 to tip x1, rounded at the tip only. An outlined bar
        is the same hue as a wash with a 1.5 px edge."""
        length = abs(x1 - x0)
        r = min(4.0, length, h / 2)
        s = 1 if x1 >= x0 else -1
        if length < 0.5:
            d = f"M{n(x0)} {n(y)}h{n(s * 0.5)}v{n(h)}h{n(-s * 0.5)}Z"
        else:
            sweep = 1 if s > 0 else 0
            d = (
                f"M{n(x0)} {n(y)}H{n(x1 - s * r)}"
                f"A{n(r)} {n(r)} 0 0 {sweep} {n(x1)} {n(y + r)}V{n(y + h - r)}"
                f"A{n(r)} {n(r)} 0 0 {sweep} {n(x1 - s * r)} {n(y + h)}H{n(x0)}Z"
            )
        hx, hy, hw, hh = hit or (min(x0, x1), y - 3, max(length, 1), h + 6)
        self.parts.append(
            f"<g><title>{html.escape(title)}</title>"
            f'<rect x="{n(hx)}" y="{n(hy)}" width="{n(hw)}" height="{n(hh)}" fill="transparent"/>'
            + (
                f'<path d="{d}" fill="{self.color(fill)}" fill-opacity="0.16" '
                f'stroke="{self.color(fill)}" stroke-width="1.5" stroke-linejoin="round"/></g>'
                if outline
                else f'<path d="{d}" fill="{self.color(fill)}"/></g>'
            )
        )

    def dot(self, x, y, fill, title, hollow=False):
        if hollow:
            mark = (
                f'<circle cx="{n(x)}" cy="{n(y)}" r="4" fill="{self.t["surface"]}" '
                f'stroke="{self.color(fill)}" stroke-width="2"/>'
            )
        else:
            mark = (
                f'<circle cx="{n(x)}" cy="{n(y)}" r="5" fill="{self.color(fill)}" '
                f'stroke="{self.t["surface"]}" stroke-width="2"/>'
            )
        self.parts.append(
            f"<g><title>{html.escape(title)}</title>"
            f'<circle cx="{n(x)}" cy="{n(y)}" r="12" fill="transparent"/>{mark}</g>'
        )

    def polyline(self, points, stroke, width=2):
        pts = " ".join(f"{n(x)},{n(y)}" for x, y in points)
        self.parts.append(
            f'<polyline points="{pts}" fill="none" stroke="{self.color(stroke)}" '
            f'stroke-width="{width}" stroke-linejoin="round" stroke-linecap="round"/>'
        )

    def swatch(self, x, y, key, label, kind="rect"):
        """A legend entry; returns its width."""
        if kind == "rect":
            self.rect(x, y - 9, 10, 10, key, rx=2)
        elif kind == "dot":
            self.parts.append(
                f'<circle cx="{n(x + 5)}" cy="{n(y - 4)}" r="5" fill="{self.color(key)}"/>'
            )
        elif kind == "ring":
            self.parts.append(
                f'<circle cx="{n(x + 5)}" cy="{n(y - 4)}" r="4" fill="none" '
                f'stroke="{self.color(key)}" stroke-width="2"/>'
            )
        self.text(x + 15, y, label, size=FS_SMALL, fill="ink2")
        return 15 + text_width(label, FS_SMALL) + 16

    def legend(self, x, y, entries):
        """entries: (key, label, kind); wraps to the width. Returns the y below it."""
        cx = x
        for key, label, kind in entries:
            w = 15 + text_width(label, FS_SMALL) + 16
            if cx + w > self.w - PAD and cx > x:
                cx, y = x, y + 20
            cx += self.swatch(cx, y, key, label, kind)
        return y + 14

    def render(self, height: float, title: str, desc: str) -> str:
        style = (
            font_face("".join(self.chars) + title + desc)
            + 'text{font-family:"Instrument Sans",ui-sans-serif,system-ui,sans-serif;'
            "font-variant-numeric:tabular-nums}"
        )
        h = math.ceil(height)
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{h}" '
            f'viewBox="0 0 {self.w} {h}" role="img" aria-labelledby="t d">'
            f'<title id="t">{html.escape(title)}</title><desc id="d">{html.escape(desc)}</desc>'
            f"<style>{style}</style>" + "".join(self.parts) + "</svg>\n"
        )


# ---- chart definitions ----------------------------------------------------------------------


@dataclass
class Measure:
    key: str
    backend: str
    title: str
    workload: str
    label: str | None
    metric: str
    unit: str
    lower_is_better: bool = True
    words: tuple[str, str] = ("faster", "slower")


LOCAL = [
    Measure("log", "local", "Logging 2,000 runs, wall time", "log", None, "wall_s", "s"),
    Measure("startup", "local", "Process-cold start-up", "startup", None, "wall_s", "s"),
    Measure(
        "startup-c",
        "local",
        "Start-up after compaction",
        "startup",
        "after_optimize",
        "wall_s",
        "s",
    ),
    Measure("series", "local", "Full series read, p50", "series", None, "full_p50_ms", "ms"),
    Measure(
        "series-c",
        "local",
        "Full series after compaction, p50",
        "series",
        "after_optimize",
        "full_p50_ms",
        "ms",
    ),
    Measure(
        "lookup",
        "local",
        "Lookup by item id, p50",
        "records_key_read",
        None,
        "lookup_p50_ms",
        "ms",
    ),
    Measure(
        "runid",
        "local",
        "Query by run id, p50",
        "records_cluster_read",
        None,
        "per_run_p50_ms",
        "ms",
    ),
    Measure("scan", "local", "Full records read", "records_key_read", None, "scan_s", "s"),
    Measure(
        "records-w",
        "local",
        "Write records, half rewritten",
        "records_key_write",
        None,
        "write_s",
        "s",
    ),
    Measure(
        "mw8",
        "local",
        "8 writers, commits per second",
        "multi_writer",
        "w8",
        "commits_per_s",
        "/s",
        lower_is_better=False,
        words=("more commits/s", "fewer commits/s"),
    ),
    Measure(
        "mw8-p99",
        "local",
        "8 writers, commit p99",
        "multi_writer",
        "w8",
        "commit_p99_ms",
        "ms",
    ),
    Measure("compact", "local", "Compaction, wall time", "optimize", None, "wall_s", "s"),
    Measure(
        "compact-mem",
        "local",
        "Compaction, peak memory",
        "optimize",
        None,
        "peak_rss_mb",
        "MB",
        words=("less memory", "more memory"),
    ),
    Measure(
        "size-c",
        "local",
        "Size on store after compaction",
        "disk",
        "size_after_optimize",
        "size_mb",
        "MB",
        words=("smaller", "larger"),
    ),
]

S3 = [
    Measure("log", "s3", "Logging 200 runs, wall time", "log", None, "wall_s", "s"),
    Measure("startup", "s3", "Process-cold start-up", "startup", None, "wall_s", "s"),
    Measure("series", "s3", "Full series read, p50", "series", None, "full_p50_ms", "ms"),
    Measure(
        "lookup", "s3", "Lookup by item id, p50", "records_key_read", None, "lookup_p50_ms", "ms"
    ),
    Measure(
        "records-w",
        "s3",
        "Write records, half rewritten",
        "records_key_write",
        None,
        "write_s",
        "s",
    ),
    Measure(
        "mw8",
        "s3",
        "8 writers, commits per second",
        "multi_writer",
        "w8",
        "commits_per_s",
        "/s",
        lower_is_better=False,
        words=("more commits/s", "fewer commits/s"),
    ),
    Measure("mw8-p99", "s3", "8 writers, commit p99", "multi_writer", "w8", "commit_p99_ms", "ms"),
    Measure("mw2-p99", "s3", "2 writers, commit p99", "multi_writer", "w2", "commit_p99_ms", "ms"),
]

BACKENDS = {"local": "Local disk", "s3": "S3-compatible server"}
WHERE = {"local": "local disk", "s3": "S3-compatible server"}


@dataclass
class Row:
    measure: Measure
    stats: dict[str, Stat]
    ratio: float = 0.0
    rival: str = ""

    def __post_init__(self):
        rivals = {
            e: s.median for e, s in self.stats.items() if e != "moraine" and s.median is not None
        }
        m = self.stats["moraine"].median
        if self.measure.lower_is_better:
            self.rival = min(rivals, key=rivals.get)
            self.ratio = rivals[self.rival] / m
        else:
            self.rival = max(rivals, key=rivals.get)
            self.ratio = m / rivals[self.rival]

    @property
    def win(self) -> bool:
        return self.ratio >= 1

    def short(self) -> str:
        good, bad = self.measure.words
        if self.win:
            return f"{fmt_ratio(self.ratio)}× {good}"
        return f"{fmt_ratio(1 / self.ratio)}× {bad}"

    def verdict(self) -> str:
        good, bad = self.measure.words
        if self.win:
            return f"{fmt_ratio(self.ratio)}× {good} than {NAMES[self.rival]}"
        return f"{fmt_ratio(1 / self.ratio)}× {bad} than {NAMES[self.rival]}"


def unit_label(unit: str) -> str:
    return {"/s": "commits/s"}.get(unit, unit)


# ---- headline: moraine against the better rival, log ratio --------------------------------


def chart_headline(rows: list[Row], theme: str, width: int) -> tuple[str, float]:
    svg = Svg(width, theme)
    narrow = width == NARROW
    groups = [(b, [r for r in rows if r.measure.backend == b]) for b in BACKENDS]
    groups = [(b, sorted(rs, key=lambda r: -r.ratio)) for b, rs in groups if rs]

    lo_r = min(r.ratio for r in rows)
    hi_r = max(r.ratio for r in rows)
    candidates = [k * 10**e for e in range(-2, 5) for k in (1, 2, 5)]
    lo = max(t for t in candidates if t <= min(lo_r / 1.15, 0.5))
    hi = min(t for t in candidates if t >= hi_r)
    if narrow:
        verdict_w = max(text_width(r.short(), FS, 600) for r in rows)
        label_w = 0
    else:
        verdict_w = max(text_width(r.verdict(), FS, 600) for r in rows)
        label_w = max(text_width(r.measure.title, FS) for r in rows) + 20
    x0 = PAD + label_w
    x1 = width - PAD - verdict_w - 10

    def x(v: float) -> float:
        return x0 + (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * (x1 - x0)

    one = x(1)
    # the key: a solid bar is a measure moraine leads, an outlined bar one it trails
    y = PAD + 4
    kx = PAD
    for outline, label in ((False, "moraine ahead"), (True, "moraine behind")):
        svg.bar(kx, kx + 22, y, 12, "moraine", label, outline=outline)
        svg.text(kx + 30, y + 11, label, size=FS_SMALL, fill="ink2")
        kx += 30 + text_width(label, FS_SMALL) + 22
    y += 26
    slower, faster = (
        ("← slower", "faster →") if narrow else ("← moraine slower", "moraine faster →")
    )
    svg.text(one - 8, y + 10, slower, size=FS_SMALL, fill="muted", anchor="end")
    svg.text(one + 8, y + 10, faster, size=FS_SMALL, fill="muted")
    y += 24
    top = y
    bar_h = 16
    row_h = 46 if narrow else 30
    marks = []
    for backend, rs in groups:
        svg.text(PAD, y + 16, BACKENDS[backend], size=FS_TITLE, weight=600, fill="ink")
        y += 28
        for r in rs:
            if narrow:
                svg.text(PAD, y + 12, r.measure.title, fill="ink2")
                svg.text(
                    PAD + text_width(r.measure.title, FS) + 6,
                    y + 12,
                    f"vs {NAMES[r.rival]}",
                    fill="muted",
                )
                by = y + 20
            else:
                svg.text(PAD, y + 12 + (bar_h - 12) / 2 + 1, r.measure.title, fill="ink2")
                by = y + 2
            marks.append((r, by))
            y += row_h
        y += 6
    bottom = y
    # ticks outward from 1x, each kept only if its label clears the last one kept
    ticks = [1.0]
    for side in (
        [t for t in candidates if 1 < t <= hi],
        [t for t in reversed(candidates) if lo <= t < 1],
    ):
        last = 1.0
        for t in side:
            need = (text_width(f"{t:,g}×", FS_SMALL) + text_width(f"{last:,g}×", FS_SMALL)) / 2 + 12
            if abs(x(t) - x(last)) >= need:
                ticks.append(t)
                last = t
    ticks.sort()
    # the loss side and the grid, drawn under everything else: across the plot when the labels
    # sit beside it, behind each bar when they sit above it
    under = Svg(width, theme)
    spans = [(by - 4, by + bar_h + 4) for _, by in marks] if narrow else [(top, bottom)]
    for ya, yb in spans:
        under.rect(x0, ya, one - x0, yb - ya, "band")
        for t in ticks:
            under.line(x(t), ya, x(t), yb, stroke="grid")
        under.line(one, ya, one, yb, stroke="axis")
    svg.parts[:0] = under.parts
    if not narrow:
        svg.line(one, top - 4, one, top, stroke="axis")
    for t in ticks:
        lab = f"{t:,g}×"
        svg.text(x(t), bottom + 16, lab, size=FS_SMALL, fill="muted", anchor="middle")
    for r, by in marks:
        m = r.stats["moraine"]
        riv = r.stats[r.rival]
        unit = unit_label(r.measure.unit)
        title = (
            f"{r.measure.title}, {WHERE[r.measure.backend]}: {r.verdict()}. "
            f"moraine {fmt(m.median)} {unit}, {NAMES[r.rival]} {fmt(riv.median)} {unit} "
            f"(medians; ratio {r.ratio:.3f})"
        )
        tip = x(r.ratio)
        svg.bar(one, tip, by, bar_h, "moraine", title, outline=not r.win)
        tx = max(tip, one) + 8
        svg.text(tx, by + 12.5, r.short() if narrow else r.verdict(), weight=600, fill="ink")
    height = bottom + 16 + 8 + 14
    if narrow:
        note = "Log scale; medians against the better of the two."
    else:
        note = (
            "Logarithmic scale; each bar compares medians with the better of Lance and Delta Lake."
        )
    svg.text(PAD, height - 4, note, size=FS_SMALL, fill="muted")
    height += 8
    wins = sum(r.win for r in rows)
    losses = [r for r in rows if not r.win]
    desc = (
        f"Bar chart, logarithmic axis centred on 1×: how many times faster moraine is than the "
        f"better of Lance and Delta Lake, {len(rows)} measures. moraine is ahead on {wins} and "
        f"behind on {len(losses)}: "
        + "; ".join(
            f"{r.measure.title} ({WHERE[r.measure.backend]}), {r.verdict()}" for r in losses
        )
        + "."
    )
    return svg.render(height, "moraine against the better of Lance and Delta Lake", desc), height


# ---- small multiples of horizontal bars ----------------------------------------------------


@dataclass
class Bar:
    name: str
    value: float | None
    key: str
    title: str
    note: str = ""


@dataclass
class Panel:
    title: str
    subtitle: str
    bars: list[Bar] = field(default_factory=list)


def draw_bar_panel(svg: Svg, p: Panel, x: float, y: float, w: float, unit: str) -> float:
    svg.text(x, y + 14, p.title, size=FS_TITLE, weight=600, fill="ink")
    y += 20
    if p.subtitle:
        svg.text(x, y + 13, p.subtitle, size=FS_SMALL, fill="muted")
        y += 20
    y += 6
    name_w = max(text_width(b.name, FS) for b in p.bars) + 10

    def label(b: Bar) -> str:
        if b.value is None:
            return b.note
        s = f"{fmt(b.value)} {unit}".strip()
        return s + (f" · {b.note}" if b.note else "")

    val_w = max(text_width(label(b), FS) for b in p.bars) + 8
    bx0, bx1 = x + name_w, x + w - val_w
    vmax = max((b.value for b in p.bars if b.value is not None), default=1)
    for b in p.bars:
        svg.text(x, y + 12, b.name, fill="ink2")
        if b.value is not None:
            tip = bx0 + max(b.value / vmax * (bx1 - bx0), 1)
            svg.bar(bx0, tip, y, 14, b.key, b.title, hit=(bx0, y - 3, bx1 - bx0, 20))
        else:
            tip = bx0
        svg.text(tip + 6, y + 12, label(b), fill="ink" if b.value is not None else "muted")
        y += 22
    svg.line(bx0, y - 22 * len(p.bars) - 3, bx0, y - 5, stroke="axis")
    return y


def grid_layout(svg: Svg, panels, y: float, draw, cols: int) -> float:
    gap = 32
    col_w = (svg.w - 2 * PAD - gap * (cols - 1)) / cols
    for i in range(0, len(panels), cols):
        bottoms = []
        for j, p in enumerate(panels[i : i + cols]):
            bottoms.append(draw(svg, p, PAD + j * (col_w + gap), y, col_w))
        y = max(bottoms) + 22
    return y


def engine_legend(svg: Svg, y: float, keys=ENGINES) -> float:
    return svg.legend(PAD, y + 12, [(k, NAMES[k], "rect") for k in keys])


def tracker_panels(rows: list[Row]) -> list[tuple[Panel, str]]:
    panels = []
    for r in rows:
        unit = unit_label(r.measure.unit)
        better = "lower is better" if r.measure.lower_is_better else "higher is better"
        sub = f"{unit}, median; {better}"
        home = r.stats["moraine"].source
        others = [NAMES[e] for e, st in r.stats.items() if st.source != home]
        if others:
            other_day = day(next(st.source for st in r.stats.values() if st.source != home))
            sub += f"; {' and '.join(others)} from the {other_day} run"
        p = Panel(r.measure.title, sub)
        for e in ENGINES:
            s = r.stats.get(e)
            if s is None:
                continue
            note = ""
            if s.failed:
                note = f"{s.failed} of {s.ok + s.failed} runs failed"
            if s.median is None:
                title = f"{NAMES[e]}: failed in {s.failed} of {s.failed} runs ({s.source})"
            else:
                title = (
                    f"{NAMES[e]}: {fmt(s.median)} {unit}, median of {s.ok}"
                    + (f", range {fmt(s.lo)}–{fmt(s.hi)}" if s.ok > 1 else "")
                    + (f", {s.failed} failed" if s.failed else "")
                    + f" ({day(s.source)} run)"
                )
            p.bars.append(Bar(NAMES[e], s.median, e, title, note))
        panels.append((p, unit))
    return panels


def chart_tracker(rows: list[Row], backend: str, theme: str, width: int):
    svg = Svg(width, theme)
    y = engine_legend(svg, PAD - 4)
    y += 8
    panels = tracker_panels(rows)
    cols = 1 if width == NARROW else 2
    y = grid_layout(
        svg, panels, y, lambda s, pu, x, yy, w: draw_bar_panel(s, pu[0], x, yy, w, pu[1]), cols
    )
    desc = (
        f"Small multiples, one per workload on {WHERE[backend]}, each with its own "
        "axis: horizontal bars for moraine, Lance and Delta Lake with the median printed at "
        "each bar."
    )
    return svg.render(y - 6, f"Tracker workloads, {WHERE[backend]}", desc), y


# ---- multi-writer scaling: lines -----------------------------------------------------------


def draw_line_panel(svg, x, y, w, title, subtitle, xs, series, log=False, notes=()):
    """series: list of (key, [(writers, value|None, tooltip)])."""
    svg.text(x, y + 14, title, size=FS_TITLE, weight=600, fill="ink")
    y += 20
    svg.text(x, y + 13, subtitle, size=FS_SMALL, fill="muted")
    y += 26
    values = [v for _, pts in series for _, v, _ in pts if v is not None]
    end_w = max(text_width(NAMES[k], FS) for k, _ in series) + 14
    if log:
        steps = [k * 10**e for e in range(-2, 6) for k in (1, 2, 5)]
        lo = max(t for t in steps if t <= min(values))
        hi = min(t for t in steps if t >= max(values))
        ticks = [t for t in steps if lo <= t <= hi]
    else:
        lo, hi = 0.0, nice_max(max(values))
        ticks = linear_ticks(hi)
    tick_w = max(text_width(fmt(t), FS_SMALL) for t in ticks) + 8
    px0, px1 = x + tick_w, x + w - end_w
    ph = 150
    py0, py1 = y + ph, y

    def yv(v):
        if log:
            return py0 - (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * ph
        return py0 - (v - lo) / (hi - lo) * ph

    def xw(wr):
        i = xs.index(wr)
        return px0 + 8 + i / (len(xs) - 1) * (px1 - px0 - 16)

    for t in ticks:
        svg.line(px0, yv(t), px1, yv(t), stroke="axis" if t == lo else "grid")
        svg.text(px0 - 6, yv(t) + 4, fmt(t), size=FS_SMALL, fill="muted", anchor="end")
    for wr in xs:
        svg.text(xw(wr), py0 + 16, str(wr), size=FS_SMALL, fill="muted", anchor="middle")
    svg.text((px0 + px1) / 2, py0 + 32, "writers", size=FS_SMALL, fill="muted", anchor="middle")
    ends = []
    for key, pts in series:
        seg: list[tuple[float, float]] = []
        for wr, v, _ in pts:
            if v is None:
                if len(seg) > 1:
                    svg.polyline(seg, key)
                seg = []
                continue
            seg.append((xw(wr), yv(v)))
        if len(seg) > 1:
            svg.polyline(seg, key)
        for wr, v, tip in pts:
            if v is not None:
                svg.dot(xw(wr), yv(v), key, tip)
        last = [(wr, v) for wr, v, _ in pts if v is not None][-1]
        if last[0] != xs[-1]:
            # a line that stops early is named where it stops, not at the right edge
            ly = yv(last[1]) - 11
            if ly - 12 < py1:  # no room above the plot's top: name it below the point
                ly = yv(last[1]) + 22
            svg.text(xw(last[0]), ly, NAMES[key], fill="ink2", anchor="middle")
            continue
        ends.append([yv(last[1]), key, xw(last[0]), yv(last[1])])
    # end labels, nudged apart and joined to their point by a leader where moved
    ends.sort()
    for i in range(1, len(ends)):
        if ends[i][0] - ends[i - 1][0] < 15:
            ends[i][0] = ends[i - 1][0] + 15
    for ly, key, ex, py in ends:
        lx = px1 + 10
        if abs(ly - py) > 0.5:
            svg.line(ex + 8, py, lx - 3, ly, stroke="muted")
        svg.text(lx, ly + 4, NAMES[key], fill="ink2")
    yy = py0 + 40
    for note in notes:
        svg.text(x, yy + 12, note, size=FS_SMALL, fill="muted")
        yy += 16
    return yy


def chart_scaling(tracker: Tracker, theme: str, width: int, metric: str):
    svg = Svg(width, theme)
    y = engine_legend(svg, PAD - 4) + 8
    cols = 1 if width == NARROW else 2
    gap = 32
    col_w = (width - 2 * PAD - gap * (cols - 1)) / cols
    bottoms = []
    sources = {}
    panels = []
    for backend in BACKENDS:
        run = tracker.scaling_run(backend)
        if run is None:
            continue
        sources[backend] = run.name
        xs = sorted(
            {
                int(r["label"][1:])
                for r in run.records
                if r["workload"] == "multi_writer" and r["backend"] == backend
            }
        )
        series, notes = [], []
        for e in ENGINES:
            pts = []
            for wr in xs:
                s = tracker._stat(run, e, backend, "multi_writer", f"w{wr}", metric)
                unit = "commits/s" if metric == "commits_per_s" else "ms"
                if s.median is None:
                    pts.append((wr, None, ""))
                else:
                    pts.append(
                        (
                            wr,
                            s.median,
                            f"{NAMES[e]}, {wr} writers: {fmt(s.median)} {unit}, median of {s.ok}"
                            + (f", range {fmt(s.lo)}–{fmt(s.hi)}" if s.ok > 1 else "")
                            + (f"; {s.failed} runs failed" if s.failed else ""),
                        )
                    )
                if s.failed:
                    what = (
                        "every run failed"
                        if s.median is None
                        else (f"{s.failed} of {s.ok + s.failed} runs failed; median of the rest")
                    )
                    notes.append(f"{NAMES[e]} at {wr} writers: {what}")
            series.append((e, pts))
        panels.append((backend, xs, series, notes))
    for i in range(0, len(panels), cols):
        row_bottoms = []
        for j, (backend, xs, series, notes) in enumerate(panels[i : i + cols]):
            if metric == "commits_per_s":
                sub = "commits per second, median, log scale; higher is better"
            else:
                sub = "commit p99 in ms, median, log scale; lower is better"
            row_bottoms.append(
                draw_line_panel(
                    svg,
                    PAD + j * (col_w + gap),
                    y,
                    col_w,
                    BACKENDS[backend],
                    sub,
                    xs,
                    series,
                    log=True,
                    notes=notes,
                )
            )
        y = max(row_bottoms) + 22
        bottoms.extend(row_bottoms)
    what = "commits per second" if metric == "commits_per_s" else "commit p99 latency"
    desc = (
        f"Line charts, one per backend: {what} against the number of concurrent writers "
        "(2, 4, 8 and 16) for moraine, Lance and Delta Lake."
    )
    return svg.render(y - 6, f"Multi-writer {what}", desc), y, sources


# ---- compaction: time and memory, two panels ----------------------------------------------


def compaction_panels(tracker: Tracker):
    """Compaction time and peak memory with moraine's default merge; superseded figures go to
    the data block only."""
    time = tracker.measure("local", "optimize", None, "wall_s")
    mem = tracker.measure("local", "optimize", None, "peak_rss_mb")
    rivals = [e for e in ENGINES[1:] if e in time or e in mem]
    if not rivals:
        return None
    out = []
    for title, stats, unit in (("Compaction time", time, "s"), ("Peak memory", mem, "MB")):
        sub = f"{unit}, median; lower is better"
        sources = {s.source for s in stats.values()}
        if len(sources) > 1:
            sub += "; runs of " + " and ".join(sorted({day(src) for src in sources}))
        p = Panel(title, sub)
        for e in ENGINES:
            s = stats.get(e)
            if s is None:
                if e == "moraine":
                    p.bars.append(Bar("moraine", None, e, "", "not yet timed on an unloaded host"))
                continue
            tip = (
                f"{NAMES[e]}: {fmt(s.median)} {unit}, median of {s.ok}"
                + (f", range {fmt(s.lo)}–{fmt(s.hi)}" if s.ok > 1 else "")
                + f" ({day(s.source)} run)"
            )
            p.bars.append(Bar(NAMES[e], s.median, e, tip))
        out.append((p, unit))
    return out, time, mem, tracker


def chart_compaction(data, theme: str, width: int):
    panels, *_ = data
    svg = Svg(width, theme)
    y = engine_legend(svg, PAD - 4) + 8
    cols = 1 if width == NARROW else 2
    y = grid_layout(
        svg, panels, y, lambda s, pu, x, yy, w: draw_bar_panel(s, pu[0], x, yy, w, pu[1]), cols
    )
    desc = (
        "Two bar charts, each with its own axis: compaction wall time in seconds and peak memory "
        "in megabytes for moraine, Lance and Delta Lake on local disk."
    )
    return svg.render(y - 6, "Compaction of the tracker tables", desc), y


# ---- vector and full-text: p50 to p99 per layout -------------------------------------------


@dataclass
class Span:
    name: str
    key: str
    p50: float
    p99: float
    tip: str


def draw_span_panel(svg: Svg, title: str, spans: list[Span], x, y, w) -> float:
    svg.text(x, y + 14, title, size=FS_TITLE, weight=600, fill="ink")
    y += 28
    name_w = max(text_width(s.name, FS) for s in spans) + 12
    val_w = max(text_width(f"{fmt(s.p50)} / {fmt(s.p99)} ms", FS) for s in spans) + 12
    bx0, bx1 = x + name_w, x + w - val_w
    hi = nice_max(max(s.p99 for s in spans))
    ticks = linear_ticks(hi, 3 if w < 300 else 4)
    hi = ticks[-1]
    rows_h = 24 * len(spans)
    for t in ticks:
        tx = bx0 + t / hi * (bx1 - bx0)
        svg.line(tx, y - 4, tx, y + rows_h - 4, stroke="axis" if t == 0 else "grid")
        svg.text(tx, y + rows_h + 10, fmt(t), size=FS_SMALL, fill="muted", anchor="middle")
    for s in spans:
        cy = y + 8
        svg.text(x, cy + 4.5, s.name, fill="ink2")
        a, b = bx0 + s.p50 / hi * (bx1 - bx0), bx0 + s.p99 / hi * (bx1 - bx0)
        svg.line(a, cy, b, cy, stroke=s.key, width=2)
        svg.dot(b, cy, s.key, s.tip, hollow=True)
        svg.dot(a, cy, s.key, s.tip)
        svg.text(bx1 + 10, cy + 4.5, f"{fmt(s.p50)} / {fmt(s.p99)} ms", fill="ink")
        y += 24
    return y + 18


def span_chart(groups, theme, width, title, desc, keys):
    svg = Svg(width, theme)
    entries = [(k, NAMES[k], "rect") for k in keys] + [
        ("ink2", "p50", "dot"),
        ("ink2", "p99", "ring"),
    ]
    y = svg.legend(PAD, PAD + 8, entries) + 10
    # one panel per row: the names and the p50 / p99 figures need the width
    cols, gap = 1, 0
    col_w = width - 2 * PAD
    for i in range(0, len(groups), cols):
        bottoms = []
        for j, (gt, spans) in enumerate(groups[i : i + cols]):
            bottoms.append(draw_span_panel(svg, gt, spans, PAD + j * (col_w + gap), y, col_w))
        y = max(bottoms) + 16
    return svg.render(y - 6, title, desc), y


def fastest_at_recall(sweep, target=0.95):
    """The setting with the lowest p50 among those reaching the recall target."""
    points = [p for p in sweep if p.get("recall_at_10", 0) >= target]
    return min(points, key=lambda p: p["p50_ms"]) if points else None


def vector_groups(runs: list[Run]):
    """Per data set: usearch F32 at each layout, and Lance's fastest setting reaching 0.95."""
    latest: dict[tuple, tuple[str, dict]] = {}
    lance: dict[str, tuple[str, dict]] = {}
    for run in runs:
        for r in run.records:
            if r.get("bench") != "vector" or "sweep" not in r:
                continue
            if r.get("config") == "usearch-f32":
                latest[(r["set"], r["files"])] = (run.name, r)
            elif r.get("kind") == "lance":
                best = fastest_at_recall(r["sweep"])
                if best is None:
                    continue
                prev = lance.get(r["set"])
                # the newest run that reached the target; within a run, the fastest setting
                if (
                    prev is None
                    or run.name > prev[0]
                    or (run.name == prev[0] and best["p50_ms"] < prev[1]["p50_ms"])
                ):
                    lance[r["set"]] = (run.name, {**best, "config": r["config"]})
    sets = sorted({s for s, _ in latest}, key=lambda s: (int(s.split("_")[0][1:]), s))
    groups, table = [], []
    for st in sets:
        dim, rows = st.split("_")
        nrows = int(rows[1:])
        title = f"{dim[1:]} dimensions, {nrows:,} rows"
        spans = []
        for files in sorted(f for s, f in latest if s == st):
            src, r = latest[(st, files)]
            p = fastest_at_recall(r["sweep"])
            if p is None:
                continue
            name = f"moraine, {files} file" + ("s" if files > 1 else "")
            tip = (
                f"{name}: p50 {fmt(p['p50_ms'])} ms, p99 {fmt(p['p99_ms'])} ms, recall@10 "
                f"{p['recall_at_10']} ({day(src)} run)"
            )
            spans.append(Span(name, "moraine", p["p50_ms"], p["p99_ms"], tip))
            table.append((title, name, p, src))
        if st in lance:
            src, p = lance[st]
            tip = (
                f"Lance IVF-PQ: p50 {fmt(p['p50_ms'])} ms, p99 {fmt(p['p99_ms'])} ms, recall@10 "
                f"{p['recall_at_10']} ({day(src)} run)"
            )
            spans.append(Span("Lance", "lance", p["p50_ms"], p["p99_ms"], tip))
            table.append((title, "Lance", p, src))
        groups.append((title, spans))
    return groups, table


def fts_groups(runs: list[Run]):
    """Per row count: moraine's sidecars at each layout and Lance; medians of the newest file."""
    latest: dict[tuple, tuple[str, list[dict]]] = {}
    for run in runs:
        by: dict[tuple, list[dict]] = {}
        for r in run.records:
            if r.get("bench") == "fts" and r["engine"] in ("moraine-sidecars", "lance-inverted"):
                by.setdefault((r["rows"], r["engine"], r["files"]), []).append(r)
        for k, rs in by.items():
            latest[k] = (run.name, rs)
    groups, table = [], []
    for nrows in sorted({k[0] for k in latest}):
        title = f"{nrows:,} rows"
        spans = []
        for engine, key in (("moraine-sidecars", "moraine"), ("lance-inverted", "lance")):
            for files in sorted(f for rr, e, f in latest if rr == nrows and e == engine):
                src, rs = latest[(nrows, engine, files)]
                p50s = [(r.get("warm") or r)["p50_ms"] for r in rs]
                p99s = [(r.get("warm") or r)["p99_ms"] for r in rs]
                p50, p99 = statistics.median(p50s), statistics.median(p99s)
                if key == "moraine":
                    name = f"moraine, {files} file" + ("s" if files > 1 else "")
                else:
                    name = "Lance"
                reps = f"median of {len(rs)}, " if len(rs) > 1 else ""
                tip = f"{name}: p50 {fmt(p50)} ms, p99 {fmt(p99)} ms ({reps}{day(src)} run)"
                spans.append(Span(name, key, p50, p99, tip))
                table.append((title, name, p50, p99, len(rs), min(p50s), max(p50s), src))
        groups.append((title, spans))
    return groups, table


# ---- blobs: throughput by value size --------------------------------------------------------


def size_name(b: int) -> str:
    return f"{b // 1048576} MiB" if b >= 1048576 else f"{b // 1024} KiB"


def blob_data(runs: list[Run]):
    latest: dict[str, tuple[str, list[dict]]] = {}
    for run in runs:
        recs = [r for r in run.records if r.get("bench") == "blobs"]
        for backend in {r["backend"] for r in recs}:
            latest[backend] = (run.name, [r for r in recs if r["backend"] == backend])
    out = {}
    for backend, (src, recs) in latest.items():
        sizes = sorted({r["size"] for r in recs})
        out[backend] = (src, {s: [r for r in recs if r["size"] == s] for s in sizes})
    return out


BLOB_OPS = (("write_mb_s", "Write"), ("read_mb_s", "Read whole"), ("stream_mb_s", "Stream"))


def chart_blobs(data, theme: str, width: int):
    svg = Svg(width, theme)
    y = PAD
    panels = []
    for metric, op in BLOB_OPS:
        for backend in ("local", "s3"):
            if backend not in data:
                continue
            _, by = data[backend]
            p = Panel(f"{op}, {WHERE[backend]}", "MB/s, median; higher is better")
            for size, rs in by.items():
                vals = [r[metric] for r in rs]
                name = size_name(size) + (", inline" if size < 262144 else "")
                tip = (
                    f"{op}, {name}, {WHERE[backend]}: {fmt(statistics.median(vals))} "
                    f"MB/s, median of {len(vals)}, range {fmt(min(vals))}–{fmt(max(vals))}"
                )
                p.bars.append(Bar(name, statistics.median(vals), "moraine", tip))
            panels.append((p, "MB/s"))
    cols = 1 if width == NARROW else 2
    y = grid_layout(
        svg, panels, y, lambda s, pu, x, yy, w: draw_bar_panel(s, pu[0], x, yy, w, pu[1]), cols
    )
    desc = (
        "Small multiples of bar charts, each with its own axis: moraine's blob write, whole-read "
        "and stream throughput in MB/s by value size, on local disk and the S3-compatible server."
    )
    return svg.render(y - 6, "moraine blob throughput by value size", desc), y


# ---- markdown -------------------------------------------------------------------------------


def src_link(name: str, prefix: str) -> str:
    return f"[`{name}`]({REPO}bench/results/{name})"


def sources_line(names) -> str:
    names = sorted(set(names))
    return "Raw results: " + ", ".join(src_link(nm, "") for nm in names) + "."


def table(head: list[str], rows: list[list[str]], align: str) -> list[str]:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join(align) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return out


def details(lines: list[str]) -> str:
    body = "\n".join(("    " + ln) if ln else "" for ln in lines)
    return f'??? note "Data"\n\n{body}\n'


def headline_data(rows: list[Row]) -> str:
    out = [
        (
            "Medians, with the range of the repetitions in brackets; the run each value comes "
            "from is in the last column."
        ),
        "",
    ]
    tr = []
    srcs = []
    for r in rows:
        unit = unit_label(r.measure.unit)
        cells = [r.measure.title]
        for e in ENGINES:
            s = r.stats.get(e)
            cells.append(s.cell(unit) if s else "–")
        cells.append(r.verdict())
        by_src: dict[str, list[str]] = {}
        for e in ENGINES:
            if e in r.stats:
                s = r.stats[e]
                by_src.setdefault(s.source, []).append(NAMES[e])
                srcs.append(s.source)
        if len(by_src) == 1:
            src = next(iter(by_src))
            cells.append(f"{day(src)}, {r.stats['moraine'].ok + r.stats['moraine'].failed} runs")
        else:
            cells.append(
                "; ".join(
                    f"{', '.join(v)}: {day(k)}, {k[11:-6]}"
                    for k, v in sorted(
                        by_src.items(),
                        key=lambda kv: ENGINES.index(
                            next(e for e in ENGINES if NAMES[e] == kv[1][0])
                        ),
                    )
                )
            )
        tr.append(cells)
    out += table(
        ["Measure", "moraine", "Lance", "Delta Lake", "moraine against the better", "Run"],
        tr,
        ["---", "---:", "---:", "---:", "---", "---"],
    )
    out += ["", sources_line(srcs)]
    return details(out)


def scaling_data(tracker: Tracker) -> str:
    out = []
    srcs = []
    for backend, where in BACKENDS.items():
        run = tracker.scaling_run(backend)
        if run is None:
            continue
        srcs.append(run.name)
        xs = sorted(
            {
                int(r["label"][1:])
                for r in run.records
                if r["workload"] == "multi_writer" and r["backend"] == backend
            }
        )
        rows = []
        for wr in xs:
            for metric, what, unit in (
                ("commits_per_s", "commits per second", ""),
                ("commit_p99_ms", "commit p99", "ms"),
            ):
                cells = [f"{wr} writers, {what}"]
                for e in ENGINES:
                    s = tracker._stat(run, e, backend, "multi_writer", f"w{wr}", metric)
                    cells.append(s.cell(unit))
                rows.append(cells)
        out += [f"**{where}**, {day(run.name)} run.", ""]
        out += table(["", "moraine", "Lance", "Delta Lake"], rows, ["---", "---:", "---:", "---:"])
        out += [""]
    out += [sources_line(srcs)]
    return details(out)


def compaction_data(data) -> str:
    _, time, mem, tracker = data
    rows = []
    srcs = []
    for label, stats, unit in (("Wall time", time, "s"), ("Peak memory", mem, "MB")):
        cells = [label]
        for e in ENGINES:
            if e in stats:
                cells.append(f"{stats[e].cell(unit)}, {day(stats[e].source)}")
                srcs.append(stats[e].source)
            else:
                cells.append("not measured on an unloaded host")
        rows.append(cells)
    out = table(
        ["Compaction of the tracker tables", "moraine", "Lance", "Delta Lake"],
        rows,
        ["---", "---:", "---:", "---:"],
    )
    old = []
    for label, metric, unit in (("wall time", "wall_s", "s"), ("peak memory", "peak_rss_mb", "MB")):
        for why, s in tracker.superseded("local", "optimize", None, metric):
            srcs.append(s.source)
            host = ", loaded host" if s.source in {r.name for r in tracker.solo} else ""
            old.append(f"{label} {s.cell(unit)} ({day(s.source)}{host})")
    if old:
        out += [
            "",
            "moraine's earlier in-memory merge, no longer the default: " + "; ".join(old) + ".",
        ]
    loaded = [run for run in tracker.solo if not tracker.not_default(run, "optimize", None)]
    if loaded and time.get("moraine") is None:
        run = max(loaded, key=lambda r: r.name)
        s = tracker._stat(run, "moraine", "local", "optimize", None, "wall_s")
        if s.median is not None:
            out += [
                "",
                (
                    f"The default (streaming) merge's wall time on the loaded host of "
                    f"{day(run.name)}: {s.cell('s')}; compare it only with the in-memory merge in "
                    "the same conditions above, not with Lance or Delta Lake."
                ),
            ]
    out += ["", sources_line(srcs)]
    return details(out)


def vector_data(table_rows) -> str:
    rows, srcs = [], []
    for title, name, p, src in table_rows:
        setting = (
            f"`expansion_search` {p['value']}"
            if "param" in p
            else f"{p['config']}, `nprobes` {p['nprobes']}, refine {p['refine']}"
        )
        rows.append(
            [
                title,
                name,
                fmt(p["p50_ms"]),
                fmt(p["p99_ms"]),
                str(p["recall_at_10"]),
                setting,
                day(src),
            ]
        )
        srcs.append(src)
    out = table(
        ["Data set", "Engine and layout", "p50 (ms)", "p99 (ms)", "Recall@10", "Setting", "Run"],
        rows,
        ["---", "---", "---:", "---:", "---:", "---", "---"],
    )
    return details(out + ["", sources_line(srcs)])


def fts_data(table_rows) -> str:
    rows, srcs = [], []
    for title, name, p50, p99, reps, lo, hi, src in table_rows:
        p50s = fmt(p50) + (f" ({fmt(lo)}–{fmt(hi)})" if reps > 1 and lo != hi else "")
        rows.append([title, name, p50s, fmt(p99), str(reps), day(src)])
        srcs.append(src)
    out = table(
        ["Rows", "Engine and layout", "p50 (ms)", "p99 (ms)", "Runs", "Run"],
        rows,
        ["---", "---", "---:", "---:", "---:", "---"],
    )
    return details(out + ["", sources_line(srcs)])


def blob_data_md(data) -> str:
    out, srcs = [], []
    for backend in ("local", "s3"):
        if backend not in data:
            continue
        src, by = data[backend]
        srcs.append(src)
        rows = []
        for size, rs in by.items():
            cells = [size_name(size) + (", inline" if size < 262144 else "")]
            for metric, _ in BLOB_OPS:
                vals = [r[metric] for r in rs]
                cells.append(stat_of(vals, 0, src).cell(""))
            for metric in ("range_4k_p50_ms", "copy_append_p50_ms"):
                cells.append(fmt(statistics.median(r[metric] for r in rs)))
            rows.append(cells)
        out += [f"**{BACKENDS[backend]}**, {len(next(iter(by.values())))} runs.", ""]
        out += table(
            [
                "Value size",
                "Write (MB/s)",
                "Read whole (MB/s)",
                "Stream (MB/s)",
                "4 KiB range p50 (ms)",
                "Copied reference p50 (ms)",
            ],
            rows,
            ["---", "---:", "---:", "---:", "---:", "---:"],
        )
        out += [""]
    out += [sources_line(srcs)]
    return details(out)


def exact_route_md(runs: list[Run]) -> str:
    latest = None
    for run in runs:
        recs = [r for r in run.records if r.get("bench") == "exact_route"]
        if recs:
            latest = (run.name, recs)
    if latest is None:
        return ""
    src, recs = latest
    rows = []
    for r in recs:
        every = r["interval_ms"]
        load = f"{r['rows_per_append']:,} rows every {every / 1000:g} s, {r['minutes']} min"
        if r.get("min_file_age_s") == 0:
            load += ", `min_file_age` 0"
        if r.get("max_steps"):
            load += f", {r['max_steps']} steps"
        rows.append(
            [
                load,
                "on" if r["before_commit"] else "off",
                f"{r['share_steady_mean'] * 100:.1f} % ({r['share_steady_max'] * 100:.1f} %)",
                f"{r['pending_share_max'] * 100:g} %",
                f"{fmt(r['append_p50_ms'])} / {fmt(r['append_p99_ms'])}",
            ]
        )
    out = table(
        [
            "Load",
            "Index before commit",
            "Exact share, mean (max)",
            "Pending, max",
            "Append p50 / p99 (ms)",
        ],
        rows,
        ["---", "---", "---:", "---:", "---:"],
    )
    return "\n".join(out + ["", sources_line([src])]) + "\n"


# ---- output -------------------------------------------------------------------------------


def figure_html(name: str, prefix: str, sizes: dict, alt: str, github: bool) -> str:
    (ww, wh), (nw, nh) = sizes["wide"], sizes["narrow"]
    a = html.escape(alt, quote=True)
    if github:
        return (
            "<picture>\n"
            f'  <source media="(prefers-color-scheme: dark)" srcset="{prefix}{name}-dark.svg">\n'
            f'  <img alt="{a}" src="{prefix}{name}-light.svg" width="{ww}">\n'
            "</picture>\n"
        )
    parts = ['<figure class="mr-chart">']
    for theme in ("light", "dark"):
        wide, narrow = f"{prefix}{name}-{theme}.svg", f"{prefix}{name}-narrow-{theme}.svg"
        parts.append(
            f'<a class="mr-chart__{theme}" href="{wide}" title="Open the chart on its own: '
            f'hover a mark for its exact value">'
            f'<picture><source media="(max-width: 40em)" srcset="{narrow}" width="{nw}" '
            f'height="{nh}"><img src="{wide}" alt="{a}" width="{ww}" height="{wh}" '
            f'loading="lazy"></picture></a>'
        )
    parts.append("</figure>")
    return "\n".join(parts) + "\n"


def replace_region(text: str, kind: str, name: str, body: str) -> str:
    pattern = re.compile(
        rf"(<!-- {kind}:{re.escape(name)} -->\n).*?(<!-- /{kind}:{re.escape(name)} -->)", re.DOTALL
    )
    return pattern.sub(lambda m: m.group(1) + "\n" + body + "\n" + m.group(2), text)


def write(path: Path, content: str):
    if not path.exists() or path.read_text() != content:
        path.write_text(content)


def main():
    runs = load_runs()
    tracker = Tracker(runs)
    rows = []
    for m in LOCAL + S3:
        stats = tracker.measure(m.backend, m.workload, m.label, m.metric)
        if "moraine" in stats and any(
            e != "moraine" and s.median is not None for e, s in stats.items()
        ):
            rows.append(Row(m, stats))

    OUT.mkdir(parents=True, exist_ok=True)
    charts = {}  # name -> (render(theme, width) -> (svg, height), alt)

    charts["headline"] = (lambda t, w: chart_headline(rows, t, w), None)
    for backend in BACKENDS:
        brows = [r for r in rows if r.measure.backend == backend]
        if brows:
            charts[f"workloads-{backend}"] = (
                lambda t, w, b=backend, br=brows: chart_tracker(br, b, t, w),
                None,
            )
    if tracker.scaling_run("local") or tracker.scaling_run("s3"):
        charts["writers-rate"] = (
            lambda t, w: chart_scaling(tracker, t, w, "commits_per_s")[:2],
            None,
        )
        charts["writers-p99"] = (
            lambda t, w: chart_scaling(tracker, t, w, "commit_p99_ms")[:2],
            None,
        )
    comp = compaction_panels(tracker)
    if comp:
        charts["compaction"] = (lambda t, w: chart_compaction(comp, t, w), None)
    vgroups, vtable = vector_groups(runs)
    if vgroups:
        charts["vector"] = (
            lambda t, w: span_chart(
                vgroups,
                t,
                w,
                "Vector search latency by data set and file layout",
                "Range plots, one per data set: p50 and p99 query latency in milliseconds at "
                "recall@10 of at least 0.95, for moraine's usearch index on 1, 10 and 100 files "
                "and for Lance's IVF-PQ index.",
                ("moraine", "lance"),
            ),
            None,
        )
    fgroups, ftable = fts_groups(runs)
    if fgroups:
        charts["fts"] = (
            lambda t, w: span_chart(
                fgroups,
                t,
                w,
                "Full-text search latency by rows and file layout",
                "Range plots, one per row count: p50 and p99 query latency in milliseconds for "
                "moraine's full-text sidecars on 1, 10 and 100 files and for Lance's index.",
                ("moraine", "lance"),
            ),
            None,
        )
    blobs = blob_data(runs)
    if blobs:
        charts["blobs"] = (lambda t, w: chart_blobs(blobs, t, w), None)

    sizes, alts = {}, {}
    expected = set()
    for name, (render, _) in charts.items():
        for theme in THEMES:
            for layout, width in (("wide", WIDE), ("narrow", NARROW)):
                svg, height = render(theme, width)
                fname = f"{name}-{theme}.svg" if layout == "wide" else f"{name}-narrow-{theme}.svg"
                expected.add(fname)
                write(OUT / fname, svg)
                sizes.setdefault(name, {})[layout] = (width, math.ceil(height))
                alts[name] = re.search(r"<desc id=\"d\">(.*?)</desc>", svg).group(1)
    for stale in OUT.glob("*.svg"):
        if stale.name not in expected:
            stale.unlink()

    data = {
        "workloads-local": headline_data([r for r in rows if r.measure.backend == "local"]),
        "workloads-s3": headline_data([r for r in rows if r.measure.backend == "s3"]),
        "writers": scaling_data(tracker),
        "compaction": compaction_data(comp) if comp else "",
        "vector": vector_data(vtable),
        "fts": fts_data(ftable),
        "blobs": blob_data_md(blobs),
        "exact-route": exact_route_md(runs),
    }
    pages = (
        (ROOT / "docs" / "benchmarks.md", "../assets/charts/", False),
        (ROOT / "docs" / "index.md", "assets/charts/", False),
        (ROOT / "README.md", "docs/assets/charts/", True),
    )
    for path, prefix, github in pages:
        text = path.read_text()
        for name in charts:
            alt = html.unescape(alts[name])
            text = replace_region(
                text, "chart", name, figure_html(name, prefix, sizes[name], alt, github)
            )
        for name, body in data.items():
            text = replace_region(text, "chart-data", name, body)
        write(path, text)


if __name__ == "__main__":
    main()
