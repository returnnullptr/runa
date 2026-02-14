import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Generator, assert_never, Callable
from weakref import WeakKeyDictionary

from greenlet import greenlet

from runa.entity import Entity
from runa.error import Error
from runa.service import Service


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
class StateChanged:
    offset: int
    state: Any


@dataclass(kw_only=True, frozen=True)
class EntityRequestReceived:
    offset: int
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityResponseSent:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class CreateEntityRequestSent:
    offset: int
    trace_offset: int
    entity_type: type[Entity]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class CreateEntityResponseReceived:
    offset: int
    request_offset: int
    entity: Entity


@dataclass(kw_only=True, frozen=True)
class EntityRequestSent:
    offset: int
    trace_offset: int
    receiver: Entity
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityResponseReceived:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class ServiceRequestSent:
    offset: int
    trace_offset: int
    service_type: type[Service]
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class ServiceResponseReceived:
    offset: int
    request_offset: int
    response: Any


@dataclass(kw_only=True, frozen=True)
class EntityErrorReceived:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class EntityErrorSent:
    offset: int
    request_offset: int
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class ServiceErrorReceived:
    offset: int
    request_offset: int
    exception: Exception


ContextMessage = (
    StateChanged
    | CreateEntityRequestReceived
    | CreateEntityResponseSent
    | EntityRequestReceived
    | EntityResponseSent
    | CreateEntityRequestSent
    | CreateEntityResponseReceived
    | EntityRequestSent
    | EntityResponseReceived
    | ServiceRequestSent
    | ServiceResponseReceived
    | EntityErrorReceived
    | EntityErrorSent
    | ServiceErrorReceived
)
InitialMessage = (
    CreateEntityRequestReceived  #
    | EntityRequestReceived
)
ExpectationMessage = (
    StateChanged
    | CreateEntityResponseSent
    | EntityResponseSent
    | CreateEntityRequestSent
    | EntityRequestSent
    | ServiceRequestSent
    | EntityErrorSent
)


class ExecutionResult:
    def __init__(self, context: list[ContextMessage]) -> None:
        self.context = context


class Runa[EntityT: Entity]:
    def __init__(self, entity_type: type[EntityT]) -> None:
        self.entity_type = entity_type
        self.entity = Entity.__new__(self.entity_type)
        self.executions: dict[int, greenlet] = {}
        self.initial_messages: dict[greenlet, InitialMessage] = {}
        self.entity_errors = WeakKeyDictionary[Error, _ErrorArguments]()
        self.context: list[ContextMessage] = []
        self._offset = 0

    def execute(self, context: list[ContextMessage]) -> ExecutionResult:
        input_deque = deque(context)
        for cached_message in self.context:
            if not input_deque or input_deque.popleft() != cached_message:
                raise NotImplementedError("Cache miss")
            self._offset = cached_message.offset + 1

        expectations = deque[ExpectationMessage]()
        while input_deque:
            event = input_deque.popleft()

            # Initial request received
            if isinstance(event, CreateEntityRequestReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = greenlet(getattr(self.entity_type, "__init__"))
                self.initial_messages[execution] = event
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            self.entity,
                            *event.args,
                            **event.kwargs,
                        ),
                    )
                )
            elif isinstance(event, EntityRequestReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = greenlet(getattr(self.entity_type, event.method_name))
                self.initial_messages[execution] = event
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            self.entity,
                            *event.args,
                            **event.kwargs,
                        ),
                    )
                )

            # Response received
            elif isinstance(event, CreateEntityResponseReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self.executions.pop(event.request_offset)
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.entity,
                        ),
                    )
                )
            elif isinstance(event, EntityResponseReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self.executions.pop(event.request_offset)
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.response,
                        ),
                    )
                )
            elif isinstance(event, ServiceResponseReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self.executions.pop(event.request_offset)
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.response,
                        ),
                    )
                )
            elif isinstance(event, EntityErrorReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                error = event.error_type(*event.args, **event.kwargs)
                self.entity_errors[error] = _ErrorArguments(
                    event.error_type,
                    event.args,
                    event.kwargs,
                )

                execution = self.executions.pop(event.request_offset)
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            event.error_type,
                            error,
                        ),
                    )
                )
            elif isinstance(event, ServiceErrorReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self.executions.pop(event.request_offset)
                self.context.append(event)
                expectations.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            type(event.exception),
                            event.exception,
                        ),
                    )
                )

            # Request sent
            elif isinstance(event, CreateEntityRequestSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)
            elif isinstance(event, EntityRequestSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)
            elif isinstance(event, ServiceRequestSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)

            # Response sent
            elif isinstance(event, CreateEntityResponseSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)
            elif isinstance(event, EntityResponseSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)
            elif isinstance(event, EntityErrorSent):
                if event != expectations.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self.context.append(event)

            # State changed
            elif isinstance(event, StateChanged):
                if expectations:
                    if event != expectations.popleft():
                        raise NotImplementedError("Inconsistent execution context")
                elif event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                getattr(self.entity, "__setstate__")(event.state)
                self.context.append(event)
                self._offset = event.offset + 1

            else:
                assert_never(event)

        self.context.extend(expectations)
        return ExecutionResult(self.context.copy())

    def _continue(
        self,
        execution: greenlet,
        switch_to_execution: Callable[[], Any],
    ) -> list[ExpectationMessage]:
        initial_message = self.initial_messages[execution]

        try:
            with Runa._intercept_interaction(self, self.entity, initial_message.offset):
                interception = switch_to_execution()
        except Error as ex:
            try:
                error_arguments = self.entity_errors[ex]
            except KeyError:
                raise ex
            return [
                EntityErrorSent(
                    offset=self._next_offset(),
                    request_offset=initial_message.offset,
                    error_type=error_arguments.error_type,
                    args=error_arguments.args,
                    kwargs=error_arguments.kwargs,
                ),
                StateChanged(
                    offset=self._next_offset(),
                    state=self.entity.__getstate__(),
                ),
            ]

        if not execution.dead:
            self.executions[interception.offset] = execution
            return [interception]

        del self.initial_messages[execution]
        if isinstance(initial_message, CreateEntityRequestReceived):
            return [
                CreateEntityResponseSent(
                    offset=self._next_offset(),
                    request_offset=initial_message.offset,
                ),
                StateChanged(
                    offset=self._next_offset(),
                    state=self.entity.__getstate__(),
                ),
            ]
        elif isinstance(initial_message, EntityRequestReceived):
            return [
                EntityResponseSent(
                    offset=self._next_offset(),
                    request_offset=initial_message.offset,
                    response=interception,
                ),
                StateChanged(
                    offset=self._next_offset(),
                    state=self.entity.__getstate__(),
                ),
            ]
        else:
            assert_never(initial_message)

    @contextmanager
    def _intercept_interaction(
        self,
        subject: Entity,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        with (
            Runa._intercept_create_entity(self, trace_offset),
            Runa._intercept_send_entity_request(self, subject, trace_offset),
            Runa._intercept_send_service_request(self, subject, trace_offset),
            Runa._intercept_entity_error(self),
            # TODO: Protect entity state
        ):
            yield

    @contextmanager
    def _intercept_create_entity(
        self,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def new(cls: type[Entity], *args: Any, **kwargs: Any) -> Entity:
            entity: Entity = main_greenlet.switch(
                CreateEntityRequestSent(
                    offset=self._next_offset(),
                    trace_offset=trace_offset,
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
        self,
        subject: Entity,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

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
                        offset=self._next_offset(),
                        trace_offset=trace_offset,
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
        self,
        subject: Entity,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

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
                        offset=self._next_offset(),
                        trace_offset=trace_offset,
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

    @contextmanager
    def _intercept_entity_error(self) -> Generator[None, None, None]:
        def new(cls: type[Error], *args: Any, **kwargs: Any) -> Error:
            error = original_new(cls, *args, **kwargs)
            self.entity_errors[error] = _ErrorArguments(cls, args, kwargs)
            return error

        original_new = Error.__new__
        setattr(Error, "__new__", new)
        try:
            yield
        finally:
            setattr(Error, "__new__", original_new)

    def _next_offset(self) -> int:
        offset = self._offset
        self._offset += 1
        return offset


class _ServiceProxy:
    pass


@dataclass
class _ErrorArguments:
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
