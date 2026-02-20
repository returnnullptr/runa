from abc import abstractmethod

from execution_completion import Execution
from execution_completion.context import (
    ContextMessage,
    CreateEntityErrorSent,
    CreateEntityRequestReceived,
    CreateEntityResponseSent,
    EntityMethodErrorSent,
    EntityMethodRequestReceived,
    EntityMethodResponseReceived,
    EntityMethodResponseSent,
    EntityStateChanged,
    ServiceMethodErrorReceived,
    ServiceMethodRequestSent,
    ServiceMethodResponseReceived,
)
from execution_completion.model import Entity, Error, Service


class Sender(Entity):
    receiver: Receiver

    def __init__(self, message: str) -> None:
        try:
            reply = self.receiver.reply(message)
        except MessageNotReceived as ex:
            raise MessageNotSent(ex.message, reason=ex.reason)

        self.replies = [reply]

    def __getstate__(self) -> list[str]:
        return self.replies.copy()

    def __setstate__(self, state: list[str]) -> None:
        self.replies = state.copy()

    def send(self, message: str) -> str:
        try:
            reply = self.receiver.reply(message=message)
        except MessageNotReceived as ex:
            raise MessageNotSent(ex.message, reason=ex.reason)
        self.replies.append(reply)
        return "Replied!"


class Receiver(Service):
    @abstractmethod
    def reply(self, message: str) -> str: ...


class MessageNotReceived(Exception):
    def __init__(self, message: str, reason: str) -> None:
        self.message = message
        self.reason = reason


class MessageNotSent(Error):
    def __init__(self, message: str, reason: str) -> None:
        self.message = message
        self.reason = reason


def test_create_entity_request_received_then_entity_method_request_sent() -> None:
    execution = Execution(Sender)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Hello!",),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        ServiceMethodRequestSent(
            offset=1,
            trace_offset=0,
            service_type=Receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_service_method_response_received_then_create_entity_response_sent() -> None:
    execution = Execution(Sender)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=1,
            trace_offset=0,
            service_type=Receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodResponseReceived(
            offset=2,
            request_offset=1,
            response="Received 'Hello!'",
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
            state=["Received 'Hello!'"],
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.replies == ["Received 'Hello!'"]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        CreateEntityRequestReceived(
            offset=0,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=1,
            trace_offset=0,
            service_type=Receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodResponseReceived(
            offset=2,
            request_offset=1,
            response="Received 'Hello!'",
        ),
        CreateEntityResponseSent(
            offset=3,
            request_offset=0,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=4,
            state=["Received 'Hello!'"],
        ),
    ]


def test_entity_method_request_received_then_service_method_request_sent() -> None:
    execution = Execution(Sender)
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=["Received 'Hello!'"],
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
        ServiceMethodRequestSent(
            offset=2,
            trace_offset=1,
            service_type=Receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_service_method_response_received_then_entity_method_response_sent() -> None:
    execution = Execution(Sender)
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=["Received 'Hello!'"],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=2,
            trace_offset=1,
            service_type=Receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
        EntityMethodResponseReceived(
            offset=3,
            request_offset=2,
            response="Received 'How are you?'",
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
            state=["Received 'Hello!'", "Received 'How are you?'"],
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.replies == ["Received 'Hello!'", "Received 'How are you?'"]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodResponseSent(
            offset=4,
            request_offset=1,
            response="Replied!",
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=5,
            state=["Received 'Hello!'", "Received 'How are you?'"],
        ),
    ]


def test_entity_method_error_received_then_create_entity_error_sent() -> None:
    execution = Execution(Sender)
    message_not_received = MessageNotReceived("Hello!", "Bad things happen")
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=1,
            trace_offset=0,
            service_type=Receiver,
            method=Receiver.reply,
            args=("Hello!",),
            kwargs={},
        ),
        ServiceMethodErrorReceived(
            offset=2,
            request_offset=1,
            exception=message_not_received,
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


def test_service_method_error_received_then_entity_method_error_sent() -> None:
    execution = Execution(Sender)
    message_not_received = MessageNotReceived("How are you?", "Bad things happen")
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=["Received 'Hello!'"],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method=Sender.send,
            args=("How are you?",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=2,
            trace_offset=1,
            service_type=Receiver,
            method=Receiver.reply,
            args=(),
            kwargs={"message": "How are you?"},
        ),
        ServiceMethodErrorReceived(
            offset=3,
            request_offset=2,
            exception=message_not_received,
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
            state=["Received 'Hello!'"],
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
            state=["Received 'Hello!'"],
        ),
    ]
