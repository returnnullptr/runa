from dataclasses import dataclass

from execution_completion import Execution
from execution_completion.context import (
    ContextMessage,
    CreateEntityErrorSent,
    CreateEntityRequestReceived,
    CreateEntityResponseSent,
    EntityMethodErrorReceived,
    EntityMethodErrorSent,
    EntityMethodRequestReceived,
    EntityMethodRequestSent,
    EntityMethodResponseReceived,
    EntityMethodResponseSent,
    EntityStateChanged,
)
from execution_completion.model import Entity, Error


@dataclass
class SenderState:
    receiver: Receiver
    replies: list[str]


class Sender(Entity):
    def __init__(self, receiver: Receiver, message: str) -> None:
        self.receiver = receiver
        try:
            reply = self.receiver.reply(message)
        except MessageNotReceived as ex:
            raise MessageNotSent(ex.message, reason=ex.reason)
        self.replies = [reply]

    def __getstate__(self) -> SenderState:
        return SenderState(self.receiver, self.replies.copy())

    def __setstate__(self, state: SenderState) -> None:
        self.receiver = state.receiver
        self.replies = state.replies.copy()

    def send(self, message: str) -> str:
        try:
            reply = self.receiver.reply(message=message)
        except MessageNotReceived as ex:
            raise MessageNotSent(ex.message, reason=ex.reason)
        self.replies.append(reply)
        return "Replied!"


class Receiver(Entity):
    def __init__(self) -> None:
        self.messages: list[str] = []

    def __getstate__(self) -> list[str]:
        return self.messages

    def __setstate__(self, state: list[str]) -> None:
        self.messages = state

    def reply(self, message: str, bad_things_happen: bool = False) -> str:
        if bad_things_happen:
            raise MessageNotReceived(message, reason="Bad things happen")
        self.messages.append(message)
        return f"Received {message!r}"


class MessageNotReceived(Error):
    def __init__(self, message: str, reason: str) -> None:
        self.message = message
        self.reason = reason


class MessageNotSent(Error):
    def __init__(self, message: str, reason: str) -> None:
        self.message = message
        self.reason = reason


def test_create_entity_request_received_then_entity_method_request_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=(receiver,),
            kwargs={"message": "Hello!"},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodRequestSent(
            offset=1,
            trace_offset=0,
            receiver=receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert receiver.messages == []

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_entity_method_response_received_then_create_entity_response_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=(receiver,),
            kwargs={"message": "Hello!"},
        ),
        EntityMethodRequestSent(
            offset=1,
            trace_offset=0,
            receiver=receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        EntityMethodResponseReceived(
            offset=2,
            request_offset=1,
            response=receiver.reply("Hello!"),
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityResponseSent(
            offset=3,
            request_offset=0,
        ),
        EntityStateChanged(
            offset=4,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.replies == ["Received 'Hello!'"]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        CreateEntityRequestReceived(
            offset=0,
            args=(receiver,),
            kwargs={"message": "Hello!"},
        ),
        EntityMethodRequestSent(
            offset=1,
            trace_offset=0,
            receiver=receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        EntityMethodResponseReceived(
            offset=2,
            request_offset=1,
            response=receiver.reply("Hello!"),
        ),
        CreateEntityResponseSent(
            offset=3,
            request_offset=0,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=4,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
    ]


def test_entity_method_request_received_then_entity_method_request_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodRequestSent(
            offset=2,
            trace_offset=1,
            receiver=receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert receiver.messages == []

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_entity_method_response_received_then_entity_method_response_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
        EntityMethodRequestSent(
            offset=2,
            trace_offset=1,
            receiver=receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
        EntityMethodResponseReceived(
            offset=3,
            request_offset=2,
            response=receiver.reply("How are you?"),
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodResponseSent(
            offset=4,
            request_offset=1,
            response="Replied!",
        ),
        EntityStateChanged(
            offset=5,
            state=SenderState(
                receiver,
                ["Received 'Hello!'", "Received 'How are you?'"],
            ),
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.replies == ["Received 'Hello!'", "Received 'How are you?'"]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        EntityStateChanged(
            offset=0,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
        EntityMethodRequestSent(
            offset=2,
            trace_offset=1,
            receiver=receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
        EntityMethodResponseReceived(
            offset=3,
            request_offset=2,
            response=receiver.reply("How are you?"),
        ),
        EntityMethodResponseSent(
            offset=4,
            request_offset=1,
            response="Replied!",
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=5,
            state=SenderState(
                receiver,
                ["Received 'Hello!'", "Received 'How are you?'"],
            ),
        ),
    ]


def test_entity_method_request_received_then_entity_method_error_sent() -> None:
    execution = Execution(Receiver)

    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=[],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={"bad_things_happen": True},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodErrorSent(
            offset=2,
            request_offset=1,
            error_type=MessageNotReceived,
            args=("Hello!",),
            kwargs={"reason": "Bad things happen"},
        ),
        EntityStateChanged(
            offset=3,
            state=[],
        ),
    ]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodErrorSent(
            offset=2,
            request_offset=1,
            error_type=MessageNotReceived,
            args=("Hello!",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=3,
            state=[],
        ),
    ]


def test_entity_method_error_received_then_create_entity_error_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=(receiver,),
            kwargs={"message": "Hello!"},
        ),
        EntityMethodRequestSent(
            offset=1,
            trace_offset=0,
            receiver=receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        EntityMethodErrorReceived(
            offset=2,
            request_offset=1,
            error_type=MessageNotReceived,
            args=("Hello!",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityErrorSent(
            offset=3,
            request_offset=0,
            error_type=MessageNotSent,
            args=("Hello!",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == input_messages + output_messages
    assert execution.context == []


def test_entity_method_error_received_then_entity_method_error_sent() -> None:
    execution = Execution(Sender)
    receiver = Receiver()
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=SenderState(receiver, ["Received 'Hello!'"]),
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
        EntityMethodRequestSent(
            offset=2,
            trace_offset=1,
            receiver=receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
        EntityMethodErrorReceived(
            offset=3,
            request_offset=2,
            error_type=MessageNotReceived,
            args=("How are you?",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodErrorSent(
            offset=4,
            request_offset=1,
            error_type=MessageNotSent,
            args=("How are you?",),
            kwargs={"reason": "Bad things happen"},
        ),
        EntityStateChanged(
            offset=5,
            state=SenderState(
                receiver,
                ["Received 'Hello!'"],
            ),
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.replies == ["Received 'Hello!'"]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodErrorSent(
            offset=4,
            request_offset=1,
            error_type=MessageNotSent,
            args=("How are you?",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=5,
            state=SenderState(
                receiver,
                ["Received 'Hello!'"],
            ),
        ),
    ]
