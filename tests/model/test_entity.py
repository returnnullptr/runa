import pytest

from execution_completion.model import Entity


def test_base_entity_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError) as exc_info:
        Entity()

    assert str(exc_info.value) == "Base 'Entity' class cannot be instantiated"


def test_init_must_be_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __getstate__(self) -> None:
                pass  # pragma: no cover

            def __setstate__(self, state: None) -> None:  # pragma: no cover
                pass  # pragma: no cover

    assert str(exc_info.value) == "'__init__' method is not implemented"


def test_getstate_must_be_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass  # pragma: no cover

            def __setstate__(self, state: None) -> None:
                pass  # pragma: no cover

    assert str(exc_info.value) == "'__getstate__' method is not implemented"


def test_setstate_must_be_implemented() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass  # pragma: no cover

            def __getstate__(self) -> None:
                pass  # pragma: no cover

    assert str(exc_info.value) == "'__setstate__' method is not implemented"


def test_getstate_return_type_annotation_required() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass  # pragma: no cover

            def __getstate__(self):  # type: ignore[no-untyped-def]
                pass  # pragma: no cover

            def __setstate__(self, state: None) -> None:
                pass  # pragma: no cover

    assert str(exc_info.value) == (
        "Missing return type annotation of '__getstate__' method"
    )


def test_getstate_return_type_and_setstate_parameter_type_must_be_consistent() -> None:
    with pytest.raises(TypeError) as exc_info:

        class InvalidEntityType(Entity):  # noqa
            def __init__(self) -> None:
                pass  # pragma: no cover

            def __getstate__(self) -> None:
                pass  # pragma: no cover

            def __setstate__(self, state: int) -> None:
                pass  # pragma: no cover

    assert str(exc_info.value) == (
        "Type of parameter 'state' of '__setstate__' method "
        "is incompatible with '__getstate__' method return type"
    )
