import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, assert_never
from uuid import uuid7

from greenlet import greenlet

from runa.entity import Entity
from runa.service import Service


@dataclass(kw_only=True, frozen=True)
class InitializeRequestReceived:
    id: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class InitializeResponseSent:
    id: str
    request_id: str

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, InitializeResponseSent)
            and self.request_id == expectation.request_id
        )


@dataclass(kw_only=True, frozen=True)
class StateChanged:
    id: str
    state: Any

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, StateChanged)  #
            and self.state == expectation.state
        )


@dataclass(kw_only=True, frozen=True)
class RequestReceived:
    id: str
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class ResponseSent:
    id: str
    request_id: str
    response: Any

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, ResponseSent)
            and self.request_id == expectation.request_id
            and self.response == expectation.response
        )


@dataclass(kw_only=True, frozen=True)
class CreateEntityRequestSent:
    id: str
    entity_type: type[Entity]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, CreateEntityRequestSent)
            and self.entity_type is expectation.entity_type
            and self.args == expectation.args
            and self.kwargs == expectation.kwargs
        )


@dataclass(kw_only=True, frozen=True)
class CreateEntityResponseReceived:
    id: str
    request_id: str
    entity: Entity


@dataclass(kw_only=True, frozen=True)
class EntityRequestSent:
    id: str
    trace_id: str
    receiver: Entity
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, EntityRequestSent)
            and self.trace_id == expectation.trace_id
            and self.receiver is expectation.receiver
            and self.method_name is expectation.method_name
            and self.args == expectation.args
            and self.kwargs == expectation.kwargs
        )


@dataclass(kw_only=True, frozen=True)
class EntityResponseReceived:
    id: str
    request_id: str
    response: Any


@dataclass(kw_only=True, frozen=True)
class ServiceRequestSent:
    id: str
    trace_id: str
    service_type: type[Service]
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def matches(self, expectation: object) -> bool:
        return (
            isinstance(expectation, ServiceRequestSent)
            and self.trace_id == expectation.trace_id
            and self.service_type is expectation.service_type
            and self.method_name == expectation.method_name
            and self.args == expectation.args
            and self.kwargs == expectation.kwargs
        )


@dataclass(kw_only=True, frozen=True)
class ServiceResponseReceived:
    id: str
    request_id: str
    response: Any


ExecutionContext = list[
    StateChanged
    | InitializeRequestReceived
    | InitializeResponseSent
    | RequestReceived
    | ResponseSent
    | CreateEntityRequestSent
    | CreateEntityResponseReceived
    | EntityRequestSent
    | EntityResponseReceived
    | ServiceRequestSent
    | ServiceResponseReceived
]

ExecutionInitialMessage = InitializeRequestReceived | RequestReceived
InterceptionMessage = CreateEntityRequestSent | EntityRequestSent | ServiceRequestSent
ExecutionFinalMessage = InitializeResponseSent | ResponseSent
Expectation = StateChanged | InterceptionMessage | ExecutionFinalMessage


class ExecutionResult:
    def __init__(self, context: ExecutionContext) -> None:
        self.context = context


class Runa[EntityT: Entity]:
    def __init__(self, entity_type: type[EntityT]) -> None:
        self.entity_type = entity_type
        self.entity = Entity.__new__(self.entity_type)
        self.executions: dict[str, greenlet] = {}
        self.initial_messages: dict[greenlet, ExecutionInitialMessage] = {}
        self.expectations = deque[Expectation]()
        self.context: ExecutionContext = []

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        for event in context:
            # Initial request received
            # - InitializeRequestReceived
            # - RequestReceived
            if isinstance(event, InitializeRequestReceived):
                execution = greenlet(getattr(self.entity_type, "__init__"))
                self.initial_messages[execution] = event
                self.context.append(event)
                self._continue(execution, self.entity, *event.args, **event.kwargs)
            elif isinstance(event, RequestReceived):
                execution = greenlet(getattr(self.entity_type, event.method_name))
                self.initial_messages[execution] = event
                self.context.append(event)
                self._continue(execution, self.entity, *event.args, **event.kwargs)

            # Response received
            # - CreateEntityResponseReceived
            # - EntityResponseReceived
            # - ServiceResponseReceived
            elif isinstance(event, CreateEntityResponseReceived):
                execution = self.executions.pop(event.request_id)
                self.context.append(event)
                self._continue(execution, event.entity)
            elif isinstance(event, EntityResponseReceived):
                execution = self.executions.pop(event.request_id)
                self.context.append(event)
                self._continue(execution, event.response)
            elif isinstance(event, ServiceResponseReceived):
                execution = self.executions.pop(event.request_id)
                self.context.append(event)
                self._continue(execution, event.response)

            # Request sent
            # - CreateEntityRequestSent
            # - EntityRequestSent
            # - ServiceRequestSent
            elif isinstance(event, CreateEntityRequestSent):
                expectation = self.expectations.popleft()
                if not event.matches(expectation):
                    raise NotImplementedError("Inconsistent execution context")
                self.executions[event.id] = self.executions.pop(expectation.id)
                self.context.append(event)
            elif isinstance(event, EntityRequestSent):
                expectation = self.expectations.popleft()
                if not event.matches(expectation):
                    raise NotImplementedError("Inconsistent execution context")
                self.executions[event.id] = self.executions.pop(expectation.id)
                self.context.append(event)
            elif isinstance(event, ServiceRequestSent):
                expectation = self.expectations.popleft()
                if not event.matches(expectation):
                    raise NotImplementedError("Inconsistent execution context")
                self.executions[event.id] = self.executions.pop(expectation.id)
                self.context.append(event)

            # Response sent
            # - InitializeResponseSent
            # - ResponseSent
            elif isinstance(event, InitializeResponseSent):
                expectation = self.expectations.popleft()
                if not event.matches(expectation):
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)
            elif isinstance(event, ResponseSent):
                expectation = self.expectations.popleft()
                if not event.matches(expectation):
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)

            # State changed
            elif isinstance(event, StateChanged):
                if self.expectations:
                    expectation = self.expectations.popleft()
                    if not event.matches(expectation):
                        raise NotImplementedError("Inconsistent execution context")
                getattr(self.entity, "__setstate__")(event.state)
                self.context.append(event)

            else:
                assert_never(event)

        self.context.extend(self.expectations)
        return ExecutionResult(self.context)

    def _continue(self, execution: greenlet, /, *args: Any, **kwargs: Any) -> None:
        initial_message = self.initial_messages[execution]

        with _intercept_interaction(self.entity, initial_message.id):
            interception = execution.switch(*args, **kwargs)

        if not execution.dead:
            self.executions[interception.id] = execution
            self.expectations.append(interception)
        else:
            del self.initial_messages[execution]
            if isinstance(initial_message, InitializeRequestReceived):
                self.expectations.append(
                    InitializeResponseSent(
                        id=_generate_event_id(),
                        request_id=initial_message.id,
                    )
                )
                self.expectations.append(
                    StateChanged(
                        id=_generate_event_id(),
                        state=self.entity.__getstate__(),
                    )
                )
            elif isinstance(initial_message, RequestReceived):
                self.expectations.append(
                    ResponseSent(
                        id=_generate_event_id(),
                        request_id=initial_message.id,
                        response=interception,
                    )
                )
                self.expectations.append(
                    StateChanged(
                        id=_generate_event_id(),
                        state=self.entity.__getstate__(),
                    )
                )


@contextmanager
def _intercept_interaction(
    subject: Entity,
    trace_id: str,
) -> Generator[None, None, None]:
    main_greenlet = greenlet.getcurrent()
    with (
        _intercept_create_entity(main_greenlet),
        _intercept_send_entity_request(main_greenlet, subject, trace_id),
        _intercept_send_service_request(main_greenlet, subject, trace_id),
        # TODO: Protect entity state
    ):
        yield


@contextmanager
def _intercept_create_entity(main_greenlet: greenlet) -> Generator[None, None, None]:
    def new(cls: type[Entity], *args: Any, **kwargs: Any) -> Entity:
        entity: Entity = main_greenlet.switch(
            CreateEntityRequestSent(
                id=_generate_event_id(),
                entity_type=cls,
                args=args,
                kwargs=kwargs,
            )
        )

        # Temporary patch __init__ to avoid double initialization
        def init(*_: Any, **__: Any) -> None:
            setattr(cls, "__init__", original_init)

        original_init = getattr(cls, "__init__")
        setattr(cls, "__init__", init)
        return entity

    original_new = Entity.__new__
    setattr(Entity, "__new__", new)
    try:
        yield
    finally:
        setattr(Entity, "__new__", original_new)


@contextmanager
def _intercept_send_entity_request(
    main_greenlet: greenlet,
    subject: Entity,
    trace_id: str,
) -> Generator[None, None, None]:
    def getattribute(entity: Entity, name: str) -> Any:
        if entity is subject:
            return original_getattribute(entity, name)

        if name.startswith("_"):
            raise AttributeError("Entity state is private")

        original_method = getattr(type(entity), name)
        if not inspect.isfunction(original_method):
            raise AttributeError("Entity state is private")

        @functools.wraps(original_method)
        def method(_: Any, /, *args: Any, **kwargs: Any) -> Any:
            return main_greenlet.switch(
                EntityRequestSent(
                    id=_generate_event_id(),
                    trace_id=trace_id,
                    receiver=entity,
                    method_name=name,
                    args=args,
                    kwargs=kwargs,
                )
            )

        return functools.partial(method, entity)

    original_getattribute = Entity.__getattribute__
    setattr(Entity, "__getattribute__", getattribute)
    try:
        yield
    finally:
        setattr(Entity, "__getattribute__", original_getattribute)


@contextmanager
def _intercept_send_service_request(
    main_greenlet: greenlet,
    subject: Entity,
    trace_id: str,
) -> Generator[None, None, None]:
    def getattribute(service: Service, name: str) -> Any:
        original_method = getattr(type(service), name)

        if name.startswith("_"):
            raise AttributeError("Service state is private")

        if not inspect.isfunction(original_method):
            raise AttributeError("Service state is private")

        @functools.wraps(original_method)
        def method(_: Any, /, *args: Any, **kwargs: Any) -> Any:
            return main_greenlet.switch(
                ServiceRequestSent(
                    id=_generate_event_id(),
                    trace_id=trace_id,
                    service_type=type(service),
                    method_name=name,
                    args=args,
                    kwargs=kwargs,
                )
            )

        return functools.partial(method, service)

    proxies: list[tuple[str, _ServiceProxy]] = []
    for attr_name, annotation in inspect.get_annotations(type(subject)).items():
        if issubclass(annotation, Service):
            proxy = _ServiceProxy()
            proxy.__class__ = annotation
            proxies.append((attr_name, proxy))

    for attr_name, service_proxy in proxies:
        setattr(subject, attr_name, service_proxy)

    original_getattribute = Service.__getattribute__
    setattr(Service, "__getattribute__", getattribute)
    try:
        yield
    finally:
        for attr_name, _ in proxies:
            delattr(subject, attr_name)
        setattr(Entity, "__getattribute__", original_getattribute)


def _generate_event_id() -> str:
    return uuid7().hex


class _ServiceProxy:
    pass
