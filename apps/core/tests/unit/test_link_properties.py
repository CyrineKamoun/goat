"""What a new project-layer link starts out looking like.

Adding a layer that is already in the project copies the style off the link
already there, so a duplicate matches what the user made rather than snapping
back to the dataset's default. `visibility` lives in that same blob, though,
and it is not style: copying it means adding a layer you had hidden gives you a
second one you also cannot see — which reads as the add having failed.
"""

from core.crud.crud_layer_project import initial_link_properties


class _Link:
    def __init__(self, properties):
        self.properties = properties


class _Layer:
    def __init__(self, properties):
        self.properties = properties


STYLED = {"color": [213, 62, 79], "opacity": 0.8, "min_zoom": 1, "visibility": False}
DEFAULT = {"color": [0, 0, 0], "opacity": 1, "visibility": True}


def test_a_duplicate_keeps_the_style_the_user_made() -> None:
    props = initial_link_properties(_Link(STYLED), _Layer(DEFAULT))

    assert props["color"] == [213, 62, 79]
    assert props["opacity"] == 0.8
    assert props["min_zoom"] == 1


def test_a_duplicate_of_a_hidden_layer_is_visible() -> None:
    props = initial_link_properties(_Link(STYLED), _Layer(DEFAULT))

    assert props["visibility"] is True


def test_the_existing_link_is_not_modified() -> None:
    existing = _Link(dict(STYLED))

    initial_link_properties(existing, _Layer(DEFAULT))

    assert existing.properties["visibility"] is False


def test_a_first_add_takes_the_dataset_s_own_style() -> None:
    props = initial_link_properties(None, _Layer(DEFAULT))

    assert props == DEFAULT


def test_a_layer_with_no_properties_at_all_is_handled() -> None:
    assert initial_link_properties(None, _Layer(None)) is None
    assert initial_link_properties(_Link(None), _Layer(None)) is None
