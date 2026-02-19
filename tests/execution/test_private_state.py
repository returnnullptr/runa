import pytest

from runa import Execution
from runa.context import ContextMessage, EntityStateChanged, EntityMethodRequestReceived
from runa.model import Entity


class Counter(Entity):
    def __init__(self, value: int) -> None:
        self.value = value

    def __getstate__(self) -> int:
        return self.value

    def __setstate__(self, state: int) -> None:
        self.value = state

    def read_private_state(self, another_counter: Counter) -> int:
        return another_counter.value


def test_read_private_state_then_error_state_is_private() -> None:
    execution = Execution(Counter)
    another_counter = Counter(2)

    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=10,
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Counter.read_private_state,
            args=(another_counter,),
            kwargs={},
        ),
    ]

    with pytest.raises(AttributeError) as exc_info:
        execution.complete(input_messages)

    assert str(exc_info.value) == "Entity state is private"
