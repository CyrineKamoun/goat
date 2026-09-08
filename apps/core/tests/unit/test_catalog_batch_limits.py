"""What a single add-from-catalog request is allowed to do.

Before this, nothing bounded a request: the ids were resolved (a dataset id
expands to every layer inside it), then every one was promoted and given a
materialize job, and only afterwards did link creation refuse the batch for
exceeding the project's 300-layer cap. A thousand ids therefore created a
thousand layers and a thousand tiling jobs before returning an error that added
nothing.
"""

import pytest
from core.endpoints.v2.project_layer import (
    MAX_CATALOG_LAYERS_PER_REQUEST,
    MAX_LAYERS_PER_PROJECT,
    catalog_batch_refusal,
)


def test_an_ordinary_add_is_allowed() -> None:
    assert catalog_batch_refusal(requested=3, existing=10) is None


def test_a_batch_at_the_limit_is_allowed() -> None:
    assert (
        catalog_batch_refusal(requested=MAX_CATALOG_LAYERS_PER_REQUEST, existing=0)
        is None
    )


def test_too_many_in_one_request_is_refused() -> None:
    refusal = catalog_batch_refusal(
        requested=MAX_CATALOG_LAYERS_PER_REQUEST + 1, existing=0
    )

    assert refusal is not None
    # The number has to be in the message: one bundle can expand to hundreds of
    # layers, so "too many" alone leaves the user with no idea what to change.
    assert str(MAX_CATALOG_LAYERS_PER_REQUEST) in refusal
    assert str(MAX_CATALOG_LAYERS_PER_REQUEST + 1) in refusal


def test_a_thousand_is_refused_before_anything_is_promoted() -> None:
    assert catalog_batch_refusal(requested=1000, existing=0) is not None


def test_a_batch_that_would_overfill_the_project_is_refused() -> None:
    refusal = catalog_batch_refusal(requested=10, existing=MAX_LAYERS_PER_PROJECT - 5)

    assert refusal is not None
    assert str(MAX_LAYERS_PER_PROJECT) in refusal


def test_filling_the_project_exactly_is_refused_like_the_link_creation_does() -> None:
    """`crud_layer_project.create` refuses at `>= 300`, so this must agree."""
    assert (
        catalog_batch_refusal(requested=1, existing=MAX_LAYERS_PER_PROJECT - 1)
        is not None
    )
    assert (
        catalog_batch_refusal(requested=1, existing=MAX_LAYERS_PER_PROJECT - 2) is None
    )


@pytest.mark.parametrize("requested", [0, 1])
def test_small_adds_to_an_empty_project_are_allowed(requested: int) -> None:
    assert catalog_batch_refusal(requested=requested, existing=0) is None
