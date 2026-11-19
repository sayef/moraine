"""Brand assets for moraine: the mark, the wordmark, banners, the social preview, the docs
colour tokens and the docs web fonts.

    uv run --no-project --with fonttools --with brotli --with uharfbuzz --with resvg-py python assets/src/build.py

Everything is drawn here, so the output is deterministic; run it again after changing anything.
Words are converted to outlines, so no asset needs a font to display. The run stops if any text
colour pairing in the assets or the docs tokens falls below WCAG AA.

The fonts are Bricolage Grotesque (the wordmark and headings), Instrument Sans (taglines and
body text) and Spline Sans Mono (code), all under the SIL Open Font License. They are downloaded
once from the google/fonts repository at a pinned commit, checked against their SHA-256, and kept
in `assets/src/.cache/` (ignored by git). The docs site serves Latin subsets as WOFF2 from
`docs/assets/fonts/`.

The identity (brand/DESIGN.md, "Mark"):
- the mark, Flow: three bands come down side by side, like concurrent writers, and each turns a
  quarter circle into a horizontal bed, like entries in the log. The bands share one centre, so
  the outermost stream becomes the oldest bed at the bottom and the innermost the newest on top.
  The newest bed is the bright ochre; older beds step down the ochre ramp.
- a separate drawing for 16 and 32 px: the same three bands on the 16 px pixel grid (2 px bands,
  1 px gaps), with straight legs long enough that the turn never reads as concentric arcs alone;
- the wordmark: `moraine` in Bricolage Grotesque, weight 780, width 88, whose i is dotted with
  one turned bed: a square with a quarter-round corner, like the mark.
"""

import hashlib
import io
import shutil
import sys
import urllib.request
from functools import lru_cache
from pathlib import Path

import resvg_py
import uharfbuzz as hb
from fontTools import subset
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "assets"
CACHE = Path(__file__).resolve().parent / ".cache"
DOCS = ROOT / "docs" / "assets"
TOKENS_CSS = ROOT / "docs" / "stylesheets" / "tokens.css"

FONTS_COMMIT = "7af0d90b4d532ffa4b041937b560c6df4e7b1e95"
BRICOLAGE = "BricolageGrotesque[opsz,wdth,wght].ttf"
INSTRUMENT = "InstrumentSans[wdth,wght].ttf"
MONO = "SplineSansMono[wght].ttf"
FONTS = {
    BRICOLAGE: (
        "ofl/bricolagegrotesque/BricolageGrotesque%5Bopsz%2Cwdth%2Cwght%5D.ttf",
        "413e7357809ddd12fd80a96a8a396de0e401638d4acd3cb3e37532f0472ac682",
    ),
    INSTRUMENT: (
        "ofl/instrumentsans/InstrumentSans%5Bwdth%2Cwght%5D.ttf",
        "b24f1812584816958afcf22e22d08e44318c5e51651e25d2438efdde389b33b1",
    ),
    MONO: (
        "ofl/splinesansmono/SplineSansMono%5Bwght%5D.ttf",
        "e20c1df32aa2f886e828cfcc81ca5c405d3f3b160990f6b892923e2e449bf525",
    ),
    "bricolage-grotesque-OFL.txt": ("ofl/bricolagegrotesque/OFL.txt", None),
    "instrument-sans-OFL.txt": ("ofl/instrumentsans/OFL.txt", None),
    "spline-sans-mono-OFL.txt": ("ofl/splinesansmono/OFL.txt", None),
}

TAGLINE = "A multi-writer database on object storage"
KEYWORDS = ["Delta Lake tables", "one PUT per commit", "vector and full-text indexes", "no server"]

# --- Colour (brand/DESIGN.md, "Colour") -----------------------------------------------------
#
# One accent family, ochre, the yellow iron oxide in glacial till. The ramp runs from light to
# dark; the mark's beds are fixed stops in both themes: the newest bed is always ochre-300, the
# middle bed ochre-500 and the oldest ochre-700, so a bed darkens with age on either ground.

OCHRE = {
    50: "#FFF7E0",
    100: "#FFEBB0",
    200: "#FFDA75",
    300: "#F9C23C",
    400: "#E5A520",
    500: "#C98B14",
    600: "#A87010",
    700: "#835A00",
    800: "#5E4108",
    900: "#3B2A0C",
}
BEDS = [OCHRE[700], OCHRE[500], OCHRE[300]]  # oldest, middle, newest

TOKENS = {  # name: (light, dark)
    "ground": ("#F7F5EF", "#15110A"),
    "ground-step": ("#EDE9DF", "#201A10"),
    "ground-raised": ("#FDFCF9", "#2A2216"),
    "ink": ("#1F1709", "#F6F0E4"),
    "ink-2": ("#4F4535", "#CBBFA8"),
    "ink-muted": ("#675C4A", "#A69881"),
    "accent": (OCHRE[700], OCHRE[300]),
    "accent-press": (OCHRE[800], OCHRE[200]),
    "on-accent": ("#FDFCF9", "#15110A"),
    "accent-tint": ("#F6E7BF", "#3D2F10"),
    "border": ("#DCD5C6", "#3E3424"),
    "success": ("#1F6E5A", "#7FCBB4"),
    "warning": ("#A3431A", "#F2915C"),
    "error": ("#B0262D", "#F28C86"),
    "focus": (OCHRE[700], OCHRE[200]),
    "bed-old": (BEDS[0], BEDS[0]),
    "bed-mid": (BEDS[1], BEDS[1]),
    "bed-new": (BEDS[2], BEDS[2]),
}

# Text pairings that must reach 4.5:1 (foreground token, background token).
TEXT_PAIRS = [
    (fg, bg)
    for bg in ("ground", "ground-step", "ground-raised")
    for fg in ("ink", "ink-2", "ink-muted", "accent", "accent-press", "success", "warning", "error")
] + [("on-accent", "accent"), ("on-accent", "accent-press"), ("ink", "accent-tint"), ("accent", "accent-tint")]
# Non-text pairings that must reach 3:1: the focus ring on every surface.
UI_PAIRS = [("focus", bg) for bg in ("ground", "ground-step", "ground-raised")]


def theme(name):
    i = 0 if name == "light" else 1
    return {k: v[i] for k, v in TOKENS.items()}


def luminance(hex_):
    def chan(c):
        c /= 255
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = (int(hex_[i : i + 2], 16) for i in (1, 3, 5))
    return 0.2126 * chan(r) + 0.7152 * chan(g) + 0.0722 * chan(b)


def contrast(a, b):
    hi, lo = sorted((luminance(a), luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def check_contrast():
    failures = []
    for name in ("light", "dark"):
        t = theme(name)
        for pairs, floor in ((TEXT_PAIRS, 4.5), (UI_PAIRS, 3.0)):
            for fg, bg in pairs:
                ratio = contrast(t[fg], t[bg])
                if ratio < floor:
                    failures.append(f"{name}: {fg} {t[fg]} on {bg} {t[bg]} is {ratio:.2f}:1, needs {floor}:1")
    if failures:
        sys.exit("WCAG AA failures:\n  " + "\n  ".join(failures))


# --- Fonts ------------------------------------------------------------------------------------


def fetch_fonts():
    CACHE.mkdir(exist_ok=True)
    for name, (path, digest) in FONTS.items():
        target = CACHE / name
        if not target.exists():
            url = f"https://raw.githubusercontent.com/google/fonts/{FONTS_COMMIT}/{path}"
            with urllib.request.urlopen(url) as response:
                target.write_bytes(response.read())
        if digest and hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            sys.exit(f"{name}: unexpected SHA-256; delete {target} and check the pinned commit")


# The Latin range served by the docs (Basic Latin, Latin-1, general punctuation, arrows used in
# prose, the minus sign); the rest of each font is dropped.
UNICODES = "U+0000-00FF,U+0131,U+0152-0153,U+02C6,U+02DA,U+02DC,U+2000-206F,U+2190-2193,U+2212,U+2248,U+2264-2265,U+00D7"


def _fresh(font):
    """Reload an instanced font: the subsetter expects a freshly parsed one."""
    buffer = io.BytesIO()
    font.save(buffer)
    buffer.seek(0)
    return TTFont(buffer)


def web_fonts():
    """Latin WOFF2 subsets for the docs: Bricolage Grotesque for headings (weights 500 to 800,
    optical size 12 to 96, width fixed at 88 as in the wordmark), Instrument Sans for text
    (weights 400 to 700, normal width) and Spline Sans Mono for code."""
    out = DOCS / "fonts"
    out.mkdir(parents=True, exist_ok=True)
    for stale in ("geologica-latin-var.woff2", "geologica-OFL.txt"):
        (out / stale).unlink(missing_ok=True)
    fonts = {
        "bricolage-grotesque-latin-var.woff2": _fresh(
            instantiateVariableFont(TTFont(CACHE / BRICOLAGE), {"wdth": 88, "wght": (500, 800), "opsz": (12, 96)})
        ),
        "instrument-sans-latin-var.woff2": _fresh(
            instantiateVariableFont(TTFont(CACHE / INSTRUMENT), {"wdth": 100, "wght": (400, 700)})
        ),
        "spline-sans-mono-latin-var.woff2": TTFont(CACHE / MONO),
    }
    for name, font in fonts.items():
        options = subset.Options()
        options.flavor = "woff2"
        options.layout_features = ["*"]
        options.name_IDs = ["*"]
        options.notdef_outline = True
        sub = subset.Subsetter(options)
        sub.populate(unicodes=subset.parse_unicodes(UNICODES))
        sub.subset(font)
        font.flavor = "woff2"
        font.save(out / name)
    for name in ("bricolage-grotesque-OFL.txt", "instrument-sans-OFL.txt", "spline-sans-mono-OFL.txt"):
        # The licence text as published, without trailing spaces.
        lines = (CACHE / name).read_text().splitlines()
        (out / name).write_text("\n".join(line.rstrip() for line in lines).rstrip() + "\n")


WORD = (BRICOLAGE, (("opsz", 96), ("wdth", 88), ("wght", 780)))
WORD_TRACKING = -0.012
TEXT = (INSTRUMENT, (("wdth", 100), ("wght", 480)))


@lru_cache(None)
def _instance(file, axes):
    return instantiateVariableFont(TTFont(CACHE / file), dict(axes))


@lru_cache(None)
def _hbfont(file, axes):
    font = hb.Font(hb.Face(hb.Blob.from_file_path(str(CACHE / file))))
    font.set_variations(dict(axes))
    return font


def fmt(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")


def _bounds(face, char, size):
    ttf = _instance(*face)
    glyphs = ttf.getGlyphSet()
    pen = BoundsPen(glyphs)
    glyphs[ttf.getBestCmap()[ord(char)]].draw(pen)
    scale = size / ttf["head"].unitsPerEm
    return tuple(v * scale for v in pen.bounds)


def text_path(face, text, x, baseline, size, tracking=0.0, dot=None):
    """Shaped outlines of `text` (with the font's kerning) as SVG path data, and the advance width.
    With `dot`, each i is drawn dotless and `dot(x0, x1, top)` gives the path of its new dot."""
    ttf = _instance(*face)
    order = ttf.getGlyphOrder()
    glyphs = ttf.getGlyphSet()
    scale = size / ttf["head"].unitsPerEm
    buf = hb.Buffer()
    buf.add_str(text)
    buf.guess_segment_properties()
    hb.shape(_hbfont(*face), buf, {"kern": True})
    parts, dots = [], []
    cursor = x
    for info, pos in zip(buf.glyph_infos, buf.glyph_positions):
        name = order[info.codepoint]
        if dot and text[info.cluster] == "i":
            name = ttf.getBestCmap()[ord("ı")]
            x0, _, x1, _ = _bounds(face, "ı", size)
            dots.append(dot(cursor + x0, cursor + x1, baseline - _bounds(face, "i", size)[3]))
        pen = SVGPathPen(glyphs, ntos=fmt)
        glyphs[name].draw(TransformPen(pen, (scale, 0, 0, -scale, cursor + pos.x_offset * scale, baseline)))
        parts.append(pen.getCommands())
        cursor += pos.x_advance * scale + tracking * size
    return " ".join(parts), cursor - x - tracking * size, " ".join(dots)


# --- The mark ---------------------------------------------------------------------------------
#
# On a 256-unit grid. Three bands share one centre. Each runs down from the top margin, turns a
# quarter circle, and runs right to the side margin. Band 0 is the outermost: the oldest bed.


def flow(n, width, gap, lo, hi):
    """Path data for each band, outermost first."""
    total = n * width + (n - 1) * gap
    cx, cy = lo + total, hi - total
    bands = []
    for i in range(n):
        r1 = total - i * (width + gap)
        r0 = r1 - width
        x0 = cx - r1
        d = f"M{fmt(x0)} {lo}V{fmt(cy)}A{fmt(r1)} {fmt(r1)} 0 0 0 {fmt(cx)} {fmt(cy + r1)}H{hi}V{fmt(cy + r0)}H{fmt(cx)}"
        if r0 > 0:
            d += f"A{fmt(r0)} {fmt(r0)} 0 0 1 {fmt(cx - r0)} {fmt(cy)}V{lo}Z"
        else:
            d += f"V{fmt(cy)}H{fmt(x0 + width)}V{lo}Z"
        bands.append(d)
    return bands


MARK = dict(n=3, width=40, gap=16, lo=24, hi=232)  # 56-unit straight legs
# 16 px: 16 units a pixel, so 2 px bands, 1 px gaps, a 1 px margin and 6 px legs.
MARK_SMALL = dict(n=3, width=32, gap=16, lo=16, hi=240)


def mark(small=False):
    bands = flow(**(MARK_SMALL if small else MARK))
    return "".join(f'<path d="{d}" fill="{c}"/>' for d, c in zip(bands, BEDS))


def turned_bed(x0, x1, top):
    """The wordmark's dot on the i: a square as wide as the stem, its lower-left corner a full
    quarter circle, so it is one bed of the mark turning."""
    w = x1 - x0
    return f"M{fmt(x0)} {fmt(top)}H{fmt(x1)}V{fmt(top + w)}H{fmt(x0 + w)}A{fmt(w)} {fmt(w)} 0 0 1 {fmt(x0)} {fmt(top)}Z"


# --- Files ------------------------------------------------------------------------------------


def svg(width, height, body, label, viewbox=None):
    vb = viewbox or f"0 0 {fmt(width)} {fmt(height)}"
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" width="{fmt(width)}" height="{fmt(height)}" '
        f'role="img" aria-label="{label}">\n<title>{label}</title>\n{body}\n</svg>\n'
    )


def png(svg_text, width, height):
    return bytes(resvg_py.svg_to_bytes(svg_string=svg_text, width=width, height=height))


def lockup(name, x, top, size):
    """The mark, edge `size`, then `moraine` with its x-height at 0.52 of the mark and its
    baseline on the mark's lowest bed. Returns the SVG body and the total width."""
    t = theme(name)
    ttf = _instance(*WORD)
    x_height = ttf["OS/2"].sxHeight / ttf["head"].unitsPerEm
    text_size = size * 0.52 / x_height
    baseline = top + size * 0.845
    word, width, dots = text_path(WORD, "moraine", x + size * 1.16, baseline, text_size, WORD_TRACKING, turned_bed)
    body = (
        f'<g transform="translate({fmt(x)} {fmt(top)}) scale({size / 256:.5f})">{mark()}</g>'
        f'<path d="{word}" fill="{t["ink"]}"/><path d="{dots}" fill="{t["bed-new"]}"/>'
    )
    return body, size * 1.16 + width


def keyword_line(face, items, size):
    """Items joined by middle dots with a set gap on each side (the font's word space is too
    tight around a dot at this size). Returns path data and width."""
    gap = size * 0.42
    parts, x = [], 0.0
    for i, item in enumerate(items):
        if i:
            d, w, _ = text_path(face, "·", x + gap, 0, size)
            parts.append(d)
            x += gap + w + gap
        d, w, _ = text_path(face, item, x, 0, size)
        parts.append(d)
        x += w
    return " ".join(parts), x


def main():
    check_contrast()
    fetch_fonts()
    web_fonts()
    OUT.mkdir(exist_ok=True)
    files = {}

    # The mark is the same in both themes (the beds are fixed ramp stops); two names are kept so
    # pages can swap them if a theme ever needs its own drawing.
    files["logo.svg"] = files["logo-dark.svg"] = svg(256, 256, mark(), "moraine", "16 16 224 224")
    files["favicon.svg"] = svg(32, 32, mark(small=True), "moraine", "0 0 256 256")

    for name, suffix in (("light", ""), ("dark", "-dark")):
        body, width = lockup(name, 4, 4, 96)
        files[f"wordmark{suffix}.svg"] = svg(round(width + 8), 104, body, "moraine")

    # The README banner: its own ground, so it reads the same on any page, with the lockup and
    # the tagline centred.
    for name in ("light", "dark"):
        t = theme(name)
        body, width = lockup(name, 0, 0, 120)
        tag, tag_width, _ = text_path(TEXT, TAGLINE, 0, 0, 30)
        files[f"banner-{name}.svg"] = svg(
            1280,
            320,
            f'<rect width="1280" height="320" rx="16" fill="{t["ground"]}"/>'
            f'<g transform="translate({fmt((1280 - width) / 2)} 66)">{body}</g>'
            f'<path transform="translate({fmt((1280 - tag_width) / 2)} 254)" d="{tag}" fill="{t["ink-2"]}"/>',
            f"moraine: {TAGLINE[0].lower()}{TAGLINE[1:]}",
        )

    # The social preview, 1280 x 640, dark: lockup, tagline, and one line of keywords in ochre.
    t = theme("dark")
    body, width = lockup("dark", 0, 0, 150)
    tag, tag_width, _ = text_path(TEXT, TAGLINE, 0, 0, 42)
    keys, keys_width = keyword_line(TEXT, KEYWORDS, 25)
    files["social-preview.svg"] = svg(
        1280,
        640,
        f'<rect width="1280" height="640" fill="{t["ground"]}"/>'
        f'<g transform="translate({fmt((1280 - width) / 2)} 176)">{body}</g>'
        f'<path transform="translate({fmt((1280 - tag_width) / 2)} 420)" d="{tag}" fill="{t["ink-2"]}"/>'
        f'<path transform="translate({fmt((1280 - keys_width) / 2)} 478)" d="{keys}" fill="{t["accent"]}"/>',
        f"moraine: {TAGLINE[0].lower()}{TAGLINE[1:]}",
    )

    for name, text in files.items():
        (OUT / name).write_text(text)
    (OUT / "social-preview.png").write_bytes(png(files["social-preview.svg"], 1280, 640))
    touch = svg(
        180, 180, f'<rect width="256" height="256" fill="{t["ground"]}"/><g transform="translate(26 26) scale(.796875)">{mark()}</g>',
        "moraine", "0 0 256 256",
    )
    (OUT / "apple-touch-icon.png").write_bytes(png(touch, 180, 180))

    # The docs site serves the same files, and its colour tokens come from this script.
    DOCS.mkdir(parents=True, exist_ok=True)
    for name in (*files, "social-preview.png", "apple-touch-icon.png"):
        shutil.copy(OUT / name, DOCS / name)
    css = ["/* Generated by assets/src/build.py from brand/DESIGN.md; edit the script, not this file. */\n"]
    for scheme, name in (("default", "light"), ("slate", "dark")):
        css.append(f'[data-md-color-scheme="{scheme}"] {{')
        css += [f"  --mr-{k}: {v.lower()};" for k, v in theme(name).items()]
        css += [f"  --mr-ochre-{k}: {v.lower()};" for k, v in OCHRE.items()]
        css.append(f"  color-scheme: {name};")
        css.append("}\n")
    TOKENS_CSS.write_text("\n".join(css))


if __name__ == "__main__":
    main()
