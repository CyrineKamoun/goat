import datetime
import gettext
from pathlib import Path

from babel.support import Translations
from jinja2 import Environment, FileSystemLoader

_BASE_DIR = Path(__file__).resolve().parent
LOCALES_DIR = str(_BASE_DIR / "locales")
TEMPLATES_DIR = str(_BASE_DIR / "templates")

_default_lang = None
DEFAULT_LANGUAGE = "en"
SUPPORTED_LANGUAGES = ["de", "en"]
GERMAN_LANGUAGE_MAP = {
    "de-at": "de",
    "de-ch": "de",
    "de-de": "de",
    "de-li": "de",
    "de-lu": "de",
}


def active_translation(lang: str) -> None:
    """
    Set active translation
    :param lang:
    :return:
    """
    global _default_lang
    lang = lang.lower() if lang else DEFAULT_LANGUAGE
    if "," in lang:
        lang = lang.split(",")[0]
    if lang in GERMAN_LANGUAGE_MAP:
        lang = GERMAN_LANGUAGE_MAP[lang]
    _default_lang = DEFAULT_LANGUAGE if lang not in SUPPORTED_LANGUAGES else lang


def current_locale() -> str:
    """
    Get current locale
    :return str:
    """
    return _default_lang or DEFAULT_LANGUAGE


def trans(message: str) -> str:
    """
    Translate message to the specified language
    :param message:
    :param lang:
    :return str:
    """
    return gettext.translation(
        "messages", localedir=LOCALES_DIR, languages=[current_locale()]
    ).gettext(message)


translations = Translations.load(LOCALES_DIR, SUPPORTED_LANGUAGES)
template_loader = FileSystemLoader(searchpath=TEMPLATES_DIR)
jinja_env = Environment(loader=template_loader, extensions=["jinja2.ext.i18n"])
jinja_env.install_gettext_translations(translations)
jinja_env.globals.update(now=datetime.datetime.utcnow, current_locale=current_locale)
