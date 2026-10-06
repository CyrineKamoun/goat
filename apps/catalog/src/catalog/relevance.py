"""How a browse list is ordered: by the published topic score."""

#: The score the harvester publishes, HIGHER IS BETTER. Ordering reads this and
#: never a tier name: the harvester owns that vocabulary, and a rename or a new
#: tier there must not need a release here.
TOPIC_SCORE_FIELD = "goat:topicRelevanceScore"

#: What an unscored row gets: the bottom of the published scale. Nothing known
#: is worth no more than known-to-be-marginal, and no less.
UNGRADED_RELEVANCE = 1

RELEVANCE_RANK_SQL = f'COALESCE("{TOPIC_SCORE_FIELD}", {UNGRADED_RELEVANCE})'

#: How much ground a row covers, as the tiebreak behind the topic score. Read
#: off the footprint, not the envelope: a dataset of metropolitan France and its
#: overseas departments has a bounding box spanning 8,624 deg² and covers 71, so
#: the envelope ranked it above everything it tied with on score. A row whose
#: geometry is still the envelope rectangle measures the same either way, and a
#: row with no geometry -- a table, or a bundle of them -- has no footprint to
#: rank by and scores 0 rather than inheriting the whole-world extent STAC
#: obliges its Collection to publish.
BBOX_AREA_SQL = "COALESCE(ST_Area(geometry), 0)"
