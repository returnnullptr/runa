from execution_completion import Execution
from execution_completion.context import (
    ContextMessage,
    CreateEntityRequestReceived,
    CreateEntityResponseSent,
    EntityMethodRequestReceived,
    EntityMethodResponseSent,
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

    def increment(self, delta: int) -> None:
        self.value += delta


def test_entity_state_changed() -> None:
    execution = Execution(Counter)
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=10,
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == []
    assert execution.context == input_messages
    assert execution.subject.value == 10

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages


def test_create_entity_request_received_then_entity_state_changed() -> None:
    execution = Execution(Counter)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=(10,),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityResponseSent(
            offset=1,
            request_offset=0,
        ),
        EntityStateChanged(
            offset=2,
            state=10,
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.value == 10

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        CreateEntityResponseSent(
            offset=1,
            request_offset=0,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=2,
            state=10,
        ),
    ]


def test_entity_method_request_received_then_entity_state_changed() -> None:
    execution = Execution(Counter)
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=10,
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Counter.increment,
            args=(32,),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodResponseSent(
            offset=2,
            request_offset=1,
            response=None,
        ),
        EntityStateChanged(
            offset=3,
            state=42,
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.value == 42

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodResponseSent(
            offset=2,
            request_offset=1,
            response=None,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=3,
            state=42,
        ),
    ]
