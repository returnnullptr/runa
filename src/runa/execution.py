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


@dataclass(kw_only=True, frozen=True)
class StateChanged:
    id: str
    state: Any


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


@dataclass(kw_only=True, frozen=True)
class CreateEntityRequestSent:
    id: str
    entity_type: type[Entity]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


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


class ExecutionResult:
    def __init__(self, context: ExecutionContext) -> None:
        self.context = context


class Runa[EntityT: Entity]:
    def __init__(self, entity_type: type[EntityT]) -> None:
        self.entity_type = entity_type
        self.entity = Entity.__new__(self.entity_type)

    def execute(
        self,
        context: ExecutionContext,
    ) -> ExecutionResult:
        executions: dict[str, greenlet] = {}
        initial: dict[greenlet, ExecutionInitialMessage] = {}
        expectations = deque[
            StateChanged | InterceptionMessage | ExecutionFinalMessage
        ]()
        main_greenlet = greenlet.getcurrent()
        result = ExecutionResult([])

        for event in context:
            if isinstance(event, StateChanged):
                if expectations:
                    expectation = expectations.popleft()
                    if (
                        not isinstance(expectation, StateChanged)
                        or event.state != expectation.state
                    ):
                        raise NotImplementedError("Inconsistent execution context")

                result.context.append(event)
                getattr(self.entity, "__setstate__")(event.state)
            elif isinstance(event, InitializeRequestReceived):
                execution = greenlet(getattr(self.entity_type, "__init__"))
                execution.switch(self.entity, *event.args, **event.kwargs)

                if not execution.dead:
                    raise NotImplementedError

                result.context.append(event)
                expectations.append(
                    InitializeResponseSent(
                        id=_generate_event_id(),
                        request_id=event.id,
                    )
                )
                expectations.append(
                    StateChanged(
                        id=_generate_event_id(),
                        state=self.entity.__getstate__(),
                    )
                )
            elif isinstance(event, InitializeResponseSent):
                expectation = expectations.popleft()
                if (
                    not isinstance(expectation, InitializeResponseSent)
                    or event.request_id != expectation.request_id
                ):
                    raise NotImplementedError("Inconsistent execution context")

                result.context.append(event)
            elif isinstance(event, RequestReceived):
                execution = greenlet(getattr(self.entity_type, event.method_name))

                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(
                        self.entity,
                        *event.args,
                        **event.kwargs,
                    )

                result.context.append(event)

                if not execution.dead:
                    initial[execution] = event
                    executions[interception.id] = execution
                    expectations.append(interception)
                else:
                    expectations.append(
                        ResponseSent(
                            id=_generate_event_id(),
                            request_id=event.id,
                            response=interception,
                        )
                    )
                    expectations.append(
                        StateChanged(
                            id=_generate_event_id(),
                            state=self.entity.__getstate__(),
                        )
                    )
            elif isinstance(event, ResponseSent):
                expectation = expectations.popleft()
                if (
                    not isinstance(expectation, ResponseSent)
                    or event.request_id != expectation.request_id
                    or event.response != expectation.response
                ):
                    raise NotImplementedError("Inconsistent execution context")

                result.context.append(event)
            elif isinstance(event, CreateEntityRequestSent):
                expectation = expectations.popleft()
                if (
                    not isinstance(expectation, CreateEntityRequestSent)
                    or event.entity_type != expectation.entity_type
                    or event.args != expectation.args
                    or event.kwargs != expectation.kwargs
                ):
                    raise NotImplementedError("Inconsistent execution context")

                executions[event.id] = executions.pop(expectation.id)
                result.context.append(event)
            elif isinstance(event, CreateEntityResponseReceived):
                result.context.append(event)

                execution = executions.pop(event.request_id)
                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(event.entity)

                if not execution.dead:
                    executions[interception.id] = execution
                    expectations.append(interception)
                else:
                    initial_event = initial[execution]
                    if isinstance(initial_event, RequestReceived):
                        expectations.append(
                            ResponseSent(
                                id=_generate_event_id(),
                                request_id=initial_event.id,
                                response=interception,
                            )
                        )
                        expectations.append(
                            StateChanged(
                                id=_generate_event_id(),
                                state=self.entity.__getstate__(),
                            )
                        )
            elif isinstance(event, EntityRequestSent):
                expectation = expectations.popleft()
                if (
                    not isinstance(expectation, EntityRequestSent)
                    or event.trace_id != expectation.trace_id
                    or event.receiver is not expectation.receiver
                    or event.method_name is not expectation.method_name
                    or event.args != expectation.args
                    or event.kwargs != expectation.kwargs
                ):
                    raise NotImplementedError("Inconsistent execution context")

                executions[event.id] = executions.pop(expectation.id)
                result.context.append(event)
            elif isinstance(event, EntityResponseReceived):
                result.context.append(event)

                execution = executions.pop(event.request_id)
                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(event.response)

                if not execution.dead:
                    executions[interception.id] = execution
                    expectations.append(interception)
                else:
                    initial_event = initial[execution]
                    if isinstance(initial_event, RequestReceived):
                        expectations.append(
                            ResponseSent(
                                id=_generate_event_id(),
                                request_id=initial_event.id,
                                response=interception,
                            )
                        )
                        expectations.append(
                            StateChanged(
                                id=_generate_event_id(),
                                state=self.entity.__getstate__(),
                            )
                        )
            elif isinstance(event, ServiceRequestSent):
                expectation = expectations.popleft()
                if not (
                    isinstance(expectation, ServiceRequestSent)
                    and event.trace_id == expectation.trace_id
                    and event.service_type is expectation.service_type
                    and event.method_name is expectation.method_name
                    and event.args == expectation.args
                    and event.kwargs == expectation.kwargs
                ):
                    raise NotImplementedError("Inconsistent execution context")

                executions[event.id] = executions.pop(expectation.id)
                result.context.append(event)
            elif isinstance(event, ServiceResponseReceived):
                result.context.append(event)

                execution = executions.pop(event.request_id)
                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(event.response)

                if not execution.dead:
                    executions[interception.id] = execution
                    expectations.append(interception)
                else:
                    initial_event = initial[execution]
                    if isinstance(initial_event, RequestReceived):
                        expectations.append(
                            ResponseSent(
                                id=_generate_event_id(),
                                request_id=initial_event.id,
                                response=interception,
                            )
                        )
                        expectations.append(
                            StateChanged(
                                id=_generate_event_id(),
                                state=self.entity.__getstate__(),
                            )
                        )
            else:
                assert_never(event)

        result.context.extend(expectations)
        return result


@contextmanager
def _intercept_interaction(
    main_greenlet: greenlet,
    subject: Entity,
    trace_id: str,
) -> Generator[None, None, None]:
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
