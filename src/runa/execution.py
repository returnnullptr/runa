import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, TypeVar, Generic
from uuid import uuid7

from greenlet import greenlet

from runa import Entity


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
    entity_type: type[Entity[Any]]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def equals_except_id(self, other: Interception) -> bool:
        return (
            isinstance(other, type(self))
            and self.entity_type == other.entity_type
            and self.args == other.args
            and self.kwargs == other.kwargs
        )


@dataclass(kw_only=True, frozen=True)
class CreateEntityResponseReceived:
    id: str
    request_id: str
    entity: Entity[Any]


@dataclass(kw_only=True, frozen=True)
class EntityRequestSent:
    id: str
    trace_id: str
    receiver: Entity[Any]
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def equals_except_id(self, other: Interception) -> bool:
        return (
            isinstance(other, type(self))
            and self.trace_id == other.trace_id
            and self.receiver is other.receiver
            and self.method_name is other.method_name
            and self.args == other.args
            and self.kwargs == other.kwargs
        )


@dataclass(kw_only=True, frozen=True)
class EntityResponseReceived:
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
]

Interception = CreateEntityRequestSent | EntityRequestSent
TraceRequest = InitializeRequestReceived | RequestReceived


class ExecutionResult:
    def __init__(self, context: ExecutionContext) -> None:
        self.context = context


EntityT = TypeVar("EntityT", bound=Entity[Any])


class Runa(Generic[EntityT]):
    def __init__(self, entity_type: type[EntityT]) -> None:
        self.entity_type = entity_type
        self.entity = Entity.__new__(self.entity_type)

    def execute(
        self,
        context: ExecutionContext,
    ) -> ExecutionResult:
        traces: dict[greenlet, TraceRequest] = {}
        executions: dict[str, greenlet] = {}
        interceptions = deque[Interception]()
        main_greenlet = greenlet.getcurrent()
        result = ExecutionResult([])

        for event in context:
            if isinstance(event, InitializeRequestReceived):
                execution = greenlet(getattr(self.entity_type, "__init__"))
                execution.switch(self.entity, *event.args, **event.kwargs)

                if not execution.dead:
                    raise NotImplementedError

                result.context.append(event)

                result.context.append(
                    InitializeResponseSent(
                        id=_generate_event_id(),
                        request_id=event.id,
                    )
                )
                result.context.append(
                    StateChanged(
                        id=_generate_event_id(),
                        state=self.entity.__getstate__(),
                    )
                )
            elif isinstance(event, StateChanged):
                result.context.append(event)
                self.entity.__setstate__(event.state)
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
                    traces[execution] = event
                    executions[interception.id] = execution
                    interceptions.append(interception)
                else:
                    result.context.append(
                        ResponseSent(
                            id=_generate_event_id(),
                            request_id=event.id,
                            response=interception,
                        )
                    )
                    result.context.append(
                        StateChanged(
                            id=_generate_event_id(),
                            state=self.entity.__getstate__(),
                        )
                    )
            elif isinstance(event, CreateEntityRequestSent):
                interception = interceptions.popleft()
                if not interception.equals_except_id(event):
                    # TODO: Raise custom error
                    raise NotImplementedError("Inconsistent execution context")

                executions[event.id] = executions.pop(interception.id)
                result.context.append(event)
            elif isinstance(event, CreateEntityResponseReceived):
                result.context.append(event)

                execution = executions.pop(event.request_id)
                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(event.entity)

                if not execution.dead:
                    executions[interception.id] = execution
                    interceptions.append(interception)
                else:
                    initial_event = traces[execution]
                    if isinstance(initial_event, RequestReceived):
                        result.context.append(
                            ResponseSent(
                                id=_generate_event_id(),
                                request_id=initial_event.id,
                                response=interception,
                            )
                        )
                        result.context.append(
                            StateChanged(
                                id=_generate_event_id(),
                                state=self.entity.__getstate__(),
                            )
                        )
            elif isinstance(event, EntityRequestSent):
                interception = interceptions.popleft()
                if not interception.equals_except_id(event):
                    # TODO: Raise custom error
                    raise NotImplementedError("Inconsistent execution context")

                executions[event.id] = executions.pop(interception.id)
                result.context.append(event)
            elif isinstance(event, EntityResponseReceived):
                result.context.append(event)

                execution = executions.pop(event.request_id)
                with _intercept_interaction(main_greenlet, self.entity, event.id):
                    interception = execution.switch(event.response)

                if not execution.dead:
                    executions[interception.id] = execution
                    interceptions.append(interception)
                else:
                    initial_event = traces[execution]
                    if isinstance(initial_event, RequestReceived):
                        result.context.append(
                            ResponseSent(
                                id=_generate_event_id(),
                                request_id=initial_event.id,
                                response=interception,
                            )
                        )
                        result.context.append(
                            StateChanged(
                                id=_generate_event_id(),
                                state=self.entity.__getstate__(),
                            )
                        )

        result.context.extend(interceptions)
        return result


@contextmanager
def _intercept_interaction(
    main_greenlet: greenlet,
    subject: Entity[Any],
    trace_id: str,
) -> Generator[None, None, None]:
    with (
        _intercept_create_entity(main_greenlet),
        _intercept_send_entity_request(main_greenlet, subject, trace_id),
        # TODO: Protect entity set
    ):
        yield


@contextmanager
def _intercept_create_entity(main_greenlet: greenlet) -> Generator[None, None, None]:
    def new(cls: type[Entity[Any]], *args: Any, **kwargs: Any) -> Entity[Any]:
        entity: Entity[Any] = main_greenlet.switch(
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
    subject: Entity[Any],
    trace_id: str,
) -> Generator[None, None, None]:
    def getattribute(entity: Entity[Any], name: str) -> Any:
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


def _generate_event_id() -> str:
    return uuid7().hex
