from runa import Execution
from runa.context import (
    ContextMessage,
    CreateEntityErrorReceived,
    CreateEntityErrorSent,
    CreateEntityRequestReceived,
    CreateEntityRequestSent,
    CreateEntityResponseReceived,
    CreateEntityResponseSent,
    EntityMethodErrorSent,
    EntityMethodRequestReceived,
    EntityMethodResponseSent,
    EntityStateChanged,
)
from runa.model import Entity, Error


class Product(Entity):
    def __init__(self, name: str, bad_things_happen: bool = False) -> None:
        if bad_things_happen:
            raise BrokenProduct(name, reason="Bad things happen")
        self.name = name

    def __getstate__(self) -> str:
        return self.name

    def __setstate__(self, state: str) -> None:
        self.name = state


class BrokenProduct(Error):
    def __init__(self, product_name: str, reason: str) -> None:
        self.product_name = product_name
        self.reason = reason


class Factory(Entity):
    def __init__(self, product_name: str) -> None:
        product = Product(product_name)
        self.products = [product]

    def __getstate__(self) -> list[Product]:
        return self.products.copy()

    def __setstate__(self, state: list[Product]) -> None:
        self.products = state.copy()

    def make(self, product_name: str) -> Product:
        product = Product(name=product_name)
        self.products.append(product)
        return product


def test_create_entity_request_received_then_create_entity_request_sent() -> None:
    execution = Execution(Factory)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Box",),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityRequestSent(
            offset=1,
            trace_offset=0,
            entity_type=Product,
            args=("Box",),
            kwargs={},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_create_entity_response_received_then_create_entity_response_sent() -> None:
    execution = Execution(Factory)
    box = Product("Box")
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Box",),
            kwargs={},
        ),
        CreateEntityRequestSent(
            offset=1,
            trace_offset=0,
            entity_type=Product,
            args=("Box",),
            kwargs={},
        ),
        CreateEntityResponseReceived(
            offset=2,
            request_offset=1,
            entity=box,
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
            state=[box],
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.products == [box]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        CreateEntityResponseSent(
            offset=3,
            request_offset=0,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=4,
            state=[box],
        ),
    ]


def test_entity_method_request_received_then_create_entity_request_sent() -> None:
    execution = Execution(Factory)
    box = Product("Box")
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=[box],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method_name="make",
            args=("Pencil",),
            kwargs={},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Product,
            args=(),
            kwargs={"name": "Pencil"},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == []
    assert execution.context == input_messages + output_messages


def test_create_entity_response_received_then_entity_method_response_sent() -> None:
    execution = Execution(Factory)
    box = Product("Box")
    pencil = Product("Pencil")
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=[box],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method_name="make",
            args=("Pencil",),
            kwargs={},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Product,
            args=(),
            kwargs={"name": "Pencil"},
        ),
        CreateEntityResponseReceived(
            offset=3,
            request_offset=2,
            entity=pencil,
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodResponseSent(
            offset=4,
            request_offset=1,
            response=pencil,
        ),
        EntityStateChanged(
            offset=5,
            state=[box, pencil],
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.products == [box, pencil]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodResponseSent(
            offset=4,
            request_offset=1,
            response=pencil,
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=5,
            state=[box, pencil],
        ),
    ]


def test_create_entity_received_then_create_entity_error_sent() -> None:
    execution = Execution(Product)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Box",),
            kwargs={"bad_things_happen": True},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityErrorSent(
            offset=1,
            request_offset=0,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        )
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == input_messages + output_messages
    assert execution.context == []


def test_create_entity_error_received_then_create_entity_error_sent() -> None:
    execution = Execution(Factory)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Box",),
            kwargs={},
        ),
        CreateEntityRequestSent(
            offset=1,
            trace_offset=0,
            entity_type=Product,
            args=("Box",),
            kwargs={},
        ),
        CreateEntityErrorReceived(
            offset=2,
            request_offset=1,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        CreateEntityErrorSent(
            offset=3,
            request_offset=0,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]
    assert execution.context == input_messages + output_messages

    processed_messages = execution.cleanup()

    assert processed_messages == input_messages + output_messages
    assert execution.context == []


def test_create_entity_error_received_then_entity_method_error_sent() -> None:
    execution = Execution(Factory)
    box = Product("Box")
    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=0,
            state=[box],
        ),
        EntityMethodRequestReceived(
            offset=1,
            method_name="make",
            args=("Pencil",),
            kwargs={},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Product,
            args=(),
            kwargs={"name": "Pencil"},
        ),
        CreateEntityErrorReceived(
            offset=3,
            request_offset=2,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodErrorSent(
            offset=4,
            request_offset=1,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        ),
        EntityStateChanged(
            offset=5,
            state=[box],
        ),
    ]
    assert execution.context == input_messages + output_messages
    assert execution.subject.products == [box]

    processed_messages = execution.cleanup()

    assert processed_messages == [
        *input_messages,
        EntityMethodErrorSent(
            offset=4,
            request_offset=1,
            error_type=BrokenProduct,
            args=("Box",),
            kwargs={"reason": "Bad things happen"},
        ),
    ]
    assert execution.context == [
        EntityStateChanged(
            offset=5,
            state=[box],
        ),
    ]
