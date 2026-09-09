"""How a browse list is ordered: the topic tier, then place, then breadth."""

TOPIC_FIELD = "goat:topicRelevance"

#: Best first. The order IS the ranking -- every weight below derives from it.
TOPIC_TIERS: tuple[str, ...] = (
    "base",
    "statutory",
    "nice_to_have",
    "low_priority",
)

#: Nothing known is worth no more than known-to-be-marginal, and no less.
UNGRADED_RELEVANCE = 1

RELEVANCE_RANK_SQL = "CASE {} ELSE {} END".format(
    " ".join(
        f"WHEN \"{TOPIC_FIELD}\" = '{tier}' THEN {len(TOPIC_TIERS) - i}"
        for i, tier in enumerate(TOPIC_TIERS)
    ),
    UNGRADED_RELEVANCE,
)

#: Where the viewer is. A stand-in for their organisation's own country, applied
#: here rather than baked into a grade, so the tier stays country-neutral.
HOME_BBOX: tuple[float, float, float, float] = (5.0, 45.0, 17.0, 56.0)
HOME_LANGUAGES: tuple[str, ...] = ("de",)

#: Wider than this and the extent is not a footprint: 5,222 of the catalog's
#: 5,232 table layers carry a world-ish box, having no geometry to state.
JUNK_SPAN_DEG = 100.0

#: An AREA, unrelated to the span above despite the equal number. A country is
#: about 72, so everything country-sized and larger ties.
BREADTH_CEILING_SQ_DEG = 100.0


def _home_rank_sql() -> str:
    """Footprint at home scores 2, language match 1, no home config 0.

    A function for that last case: an empty language list compiled to
    ``language_code IN ()``, a syntax error, and emptying it is how this is
    turned off.
    """
    branches = []
    if HOME_BBOX:
        west, south, east, north = HOME_BBOX
        branches.append(
            f"WHEN bbox_xmin >= {west} AND bbox_xmax <= {east}"
            f" AND bbox_ymin >= {south} AND bbox_ymax <= {north} THEN 2"
        )
    if HOME_LANGUAGES:
        codes = ", ".join(f"'{code}'" for code in HOME_LANGUAGES)
        branches.append(f"WHEN language_code IN ({codes}) THEN 1")
    if not branches:
        return "0"
    return "CASE " + " ".join(branches) + " ELSE 0 END"


HOME_RANK_SQL = _home_rank_sql()

#: Breadth inside a tier: four bands leave thousands sharing a score, and extent
#: area separates them for nothing.
SIZE_RANK_SQL = (
    "COALESCE(LEAST(CASE WHEN"
    " GREATEST(bbox_xmax - bbox_xmin, bbox_ymax - bbox_ymin)"
    f" > {JUNK_SPAN_DEG} THEN 0"
    " ELSE (bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin) END,"
    f" {BREADTH_CEILING_SQ_DEG}), 0)"
)
