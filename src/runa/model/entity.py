import inspect
from typing import Any, Self


class Entity:
    def __init_subclass__(cls) -> None:
        if cls.__init__ is object.__init__:
            raise TypeError("'__init__' method is not implemented")

        if cls.__getstate__ is object.__getstate__:
            raise TypeError("'__getstate__' method is not implemented")

        getstate_signature = inspect.signature(cls.__getstate__)
        if getstate_signature.return_annotation is inspect.Parameter.empty:
            raise TypeError("Missing return type annotation of '__getstate__' method")

        if not hasattr(cls, "__setstate__"):
            raise TypeError("'__setstate__' method is not implemented")

        setstate_signature = inspect.signature(getattr(cls, "__setstate__"))
        _, state_parameter = setstate_signature.parameters.values()
        if state_parameter.annotation != getstate_signature.return_annotation:
            raise TypeError(
                f"Type of parameter {state_parameter.name!r} of '__setstate__' method "
                "is incompatible with '__getstate__' method return type"
            )

        super().__init_subclass__()

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls is Entity:
            raise TypeError("Base 'Entity' class cannot be instantiated")
        return super().__new__(cls)
