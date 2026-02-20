import pytest

from execution_completion import Execution
from execution_completion.context import (
    ContextMessage,
    EntityMethodRequestReceived,
    EntityStateChanged,
)
from execution_completion.model import Entity


class Counter(Entity):
    def __init__(self, value: int) -> None:
        self.value = value

    def __getstate__(self) -> int:
        return self.value

    def __setstate__(self, state: int) -> None:
        self.value = state

    def read_private_state(self, another_counter: Counter) -> int:
        return another_counter.value

    def modify_private_state(self, another_counter: Counter) -> None:
        another_counter.value = self.value

    def call_protected_method(self, another_counter: Counter) -> None:
        another_counter._increment()

    def _increment(self) -> None:
        self.value += 1


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


def test_modify_private_state_then_error_state_is_private() -> None:
    execution = Execution(Counter)
    another_counter = Counter(2)

    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=10,
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Counter.modify_private_state,
            args=(another_counter,),
            kwargs={},
        ),
    ]

    with pytest.raises(AttributeError) as exc_info:
        execution.complete(input_messages)

    assert str(exc_info.value) == "Entity state is private"


def test_call_protected_method_then_error_state_is_private() -> None:
    execution = Execution(Counter)
    another_counter = Counter(2)

    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=10,
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Counter.call_protected_method,
            args=(another_counter,),
            kwargs={},
        ),
    ]

    with pytest.raises(AttributeError) as exc_info:
        execution.complete(input_messages)

    assert str(exc_info.value) == "Entity state is private"
