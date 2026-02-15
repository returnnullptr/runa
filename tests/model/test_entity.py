import pytest

from runa.model import Entity


def test_base_entity_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError) as exc_info:
        Entity()

    assert str(exc_info.value) == "Base 'Entity' class cannot be instantiated"


def test_init_is_not_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __getstate__(self) -> None:
                pass

            def __setstate__(self, state: None) -> None:
                pass

    assert str(exc_info.value) == "'__init__' method is not implemented"


def test_getstate_is_not_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass

            def __setstate__(self, state: None) -> None:
                pass

    assert str(exc_info.value) == "'__getstate__' method is not implemented"


def test_setstate_is_not_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass

            def __getstate__(self) -> None:
                pass

    assert str(exc_info.value) == "'__setstate__' method is not implemented"


def test_missing_getstate_return_type_annotation() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass

            def __getstate__(self):  # type: ignore[no-untyped-def]
                pass

            def __setstate__(self, state: None) -> None:
                pass

    assert str(exc_info.value) == (
        "Missing return type annotation of '__getstate__' method"
    )


def test_incompatible_setstate_parameter_type() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass

            def __getstate__(self) -> None:
                pass

            def __setstate__(self, state: int) -> None:
                pass

    assert str(exc_info.value) == (
        "Type of parameter 'state' of '__setstate__' method "
        "is incompatible with '__getstate__' method return type"
    )
