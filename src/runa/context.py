from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Union

from runa.model import Entity, Error, Service


@dataclass(kw_only=True, frozen=True)
class CreateEntityRequestSent:
    offset: int
    trace_offset: int
    entity_type: type[Entity]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class CreateEntityRequestReceived:
    offset: int
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class CreateEntityResponseSent:
    offset: int
    request_offset: int


@dataclass(kw_only=True, frozen=True)
class CreateEntityResponseReceived:
    offset: int
    request_offset: int
    response: Entity


@dataclass(kw_only=True, frozen=True)
class CreateEntityErrorSent:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class CreateEntityErrorReceived:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityMethodRequestSent:
    offset: int
    trace_offset: int
    receiver: Entity
    method: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityMethodRequestReceived:
    offset: int
    method: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityMethodResponseSent:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class EntityMethodResponseReceived:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class EntityMethodErrorSent:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityMethodErrorReceived:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class ServiceMethodRequestSent:
    offset: int
    trace_offset: int
    service_type: type[Service]
    method: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class ServiceMethodResponseReceived:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class ServiceMethodErrorReceived:
    offset: int
    request_offset: int
    exception: Exception


@dataclass(kw_only=True, frozen=True)
class EntityStateChanged:
    offset: int
    state: Any


ContextMessage = Union[
    CreateEntityRequestSent,
    CreateEntityRequestReceived,
    CreateEntityResponseSent,
    CreateEntityResponseReceived,
    CreateEntityErrorSent,
    CreateEntityErrorReceived,
    EntityMethodRequestSent,
    EntityMethodRequestReceived,
    EntityMethodResponseSent,
    EntityMethodResponseReceived,
    EntityMethodErrorSent,
    EntityMethodErrorReceived,
    ServiceMethodRequestSent,
    ServiceMethodResponseReceived,
    ServiceMethodErrorReceived,
    EntityStateChanged,
]
InitiatorMessage = Union[
    CreateEntityRequestReceived,
    EntityMethodRequestReceived,
]
OutputMessage = Union[
    CreateEntityRequestSent,
    CreateEntityResponseSent,
    CreateEntityErrorSent,
    EntityMethodRequestSent,
    EntityMethodResponseSent,
    EntityMethodErrorSent,
    ServiceMethodRequestSent,
    EntityStateChanged,
]
REQUEST_RECEIVED = (
    CreateEntityRequestReceived,
    EntityMethodRequestReceived,
)
RESPONSE_SENT = (
    CreateEntityResponseSent,
    EntityMethodResponseSent,
)
ERROR_SENT = (
    CreateEntityErrorSent,
    EntityMethodErrorSent,
)
REQUEST_SENT = (
    CreateEntityRequestSent,
    EntityMethodRequestSent,
    ServiceMethodRequestSent,
)
RESPONSE_RECEIVED = (
    CreateEntityResponseReceived,
    EntityMethodResponseReceived,
    ServiceMethodResponseReceived,
)
ERROR_RECEIVED = (
    CreateEntityErrorReceived,
    EntityMethodErrorReceived,
    ServiceMethodErrorReceived,
)
