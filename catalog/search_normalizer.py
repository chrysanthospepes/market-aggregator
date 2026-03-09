from __future__ import annotations

import re
import unicodedata


_MULTISPACE_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^0-9a-zA-Zα-ωΑ-Ωάέίόύήώϊϋΐΰς\s]", re.UNICODE)

_DIGRAPH_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("ου", "ou"),
    ("αι", "ai"),
    ("ει", "ei"),
    ("οι", "oi"),
    ("υι", "yi"),
    ("γγ", "ng"),
    ("γκ", "gk"),
    ("γξ", "nx"),
    ("γχ", "nch"),
    ("τσ", "ts"),
    ("τζ", "tz"),
    ("μπ", "mp"),
    ("ντ", "nt"),
)

_GREEK_TO_LATIN = {
    "α": "a",
    "β": "v",
    "γ": "g",
    "δ": "d",
    "ε": "e",
    "ζ": "z",
    "η": "i",
    "θ": "th",
    "ι": "i",
    "κ": "k",
    "λ": "l",
    "μ": "m",
    "ν": "n",
    "ξ": "x",
    "ο": "o",
    "π": "p",
    "ρ": "r",
    "σ": "s",
    "ς": "s",
    "τ": "t",
    "υ": "y",
    "φ": "f",
    "χ": "ch",
    "ψ": "ps",
    "ω": "o",
}

_LATIN_PHONETIC_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("ph", "f"),
    ("ch", "x"),
    ("ou", "u"),
    ("oi", "i"),
    ("ei", "i"),
    ("yi", "i"),
    ("ai", "e"),
    ("ck", "k"),
)


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_search_text(value: str) -> str:
    lowered = _strip_accents((value or "").lower().replace("\xa0", " "))
    collapsed = _NON_ALNUM_RE.sub(" ", lowered)
    return _MULTISPACE_RE.sub(" ", collapsed).strip()


def transliterate_greek_to_latin(value: str) -> str:
    text = normalize_search_text(value)
    for src, dst in _DIGRAPH_REPLACEMENTS:
        text = text.replace(src, dst)
    translated = "".join(_GREEK_TO_LATIN.get(ch, ch) for ch in text)
    return _MULTISPACE_RE.sub(" ", translated).strip()


def normalize_greeklish_latin(value: str) -> str:
    text = normalize_search_text(value)
    for src, dst in _LATIN_PHONETIC_REPLACEMENTS:
        text = text.replace(src, dst)
    return _MULTISPACE_RE.sub(" ", text).strip()


def build_search_text(value: str) -> str:
    base = normalize_search_text(value)
    if not base:
        return ""

    forms = build_search_forms(base)
    return " ".join(forms)


def build_search_forms(value: str) -> list[str]:
    base = normalize_search_text(value)
    if not base:
        return []

    forms: list[str] = [base]
    latin = transliterate_greek_to_latin(base)
    if latin and latin not in forms:
        forms.append(latin)
    phonetic = normalize_greeklish_latin(latin)
    if phonetic and phonetic not in forms:
        forms.append(phonetic)
    return forms
