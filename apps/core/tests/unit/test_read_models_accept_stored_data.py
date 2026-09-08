"""A read model must accept anything the database can hold.

The same classes serve as storage model, input schema and response schema, so a
rule meant for input — `max_length`, a format assertion — also runs when a row
is turned back into a response. When stored data violates it there is no way
out: the row exists, the user cannot edit it, and the whole response fails. One
catalog layer with a 2,440-character description stopped an entire project from
loading that way.

Length belongs on input. This test fails when a constraint reaches the read
path, which is the only reliable way to keep it off: nothing else notices until
data that violates it arrives.
"""

import pytest
from annotated_types import MaxLen, MinLen
from core.schemas.layer import (
    IFeatureStandardLayerRead,
    IFeatureStreetNetworkLayerRead,
    IFeatureToolLayerRead,
    IRasterLayerRead,
    ITableLayerRead,
)
from core.schemas.project import (
    IFeatureStandardProjectRead,
    IFeatureStreetNetworkProjectRead,
    IFeatureToolProjectRead,
    ILayerProjectGroupRead,
    IRasterProjectRead,
    ITableProjectRead,
)

READ_MODELS = [
    IFeatureStandardLayerRead,
    IFeatureToolLayerRead,
    IFeatureStreetNetworkLayerRead,
    ITableLayerRead,
    IRasterLayerRead,
    IFeatureStandardProjectRead,
    IFeatureToolProjectRead,
    IFeatureStreetNetworkProjectRead,
    ITableProjectRead,
    IRasterProjectRead,
    ILayerProjectGroupRead,
]

#: Fields whose value this service produces rather than stores — a constraint
#: on one of these cannot be violated by data arriving from elsewhere.
SELF_PRODUCED: set[str] = set()


@pytest.mark.parametrize("model", READ_MODELS, ids=lambda m: m.__name__)
def test_no_read_field_bounds_the_length_of_stored_data(model: type) -> None:
    bounded = {
        name: [
            type(meta).__name__
            for meta in field.metadata
            if isinstance(meta, (MaxLen, MinLen))
        ]
        for name, field in model.model_fields.items()
        if name not in SELF_PRODUCED
        and any(isinstance(meta, (MaxLen, MinLen)) for meta in field.metadata)
    }

    assert not bounded, (
        f"{model.__name__} would refuse to serialise a row whose {sorted(bounded)} "
        "exceeds a length the database happily stored. Put the bound on the "
        "create/update schema instead, and override the field on the read model."
    )
