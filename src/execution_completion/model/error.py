from typing import Any, Self


class Error(Exception):
    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        return super().__new__(cls)
