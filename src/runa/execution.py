import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable, assert_never
from weakref import WeakKeyDictionary

from greenlet import greenlet

from runa.context import (
    REQUEST_SENT,
    RESPONSE_RECEIVED,
    RESPONSE_SENT,
    ContextMessage,
    CreateEntityErrorReceived,
    CreateEntityErrorSent,
    CreateEntityRequestReceived,
    CreateEntityRequestSent,
    CreateEntityResponseReceived,
    CreateEntityResponseSent,
    EntityMethodErrorReceived,
    EntityMethodErrorSent,
    EntityMethodRequestReceived,
    EntityMethodRequestSent,
    EntityMethodResponseReceived,
    EntityMethodResponseSent,
    EntityStateChanged,
    InitiatorMessage,
    OutputMessage,
    ServiceMethodErrorReceived,
    ServiceMethodRequestSent,
    ServiceMethodResponseReceived,
)
from runa.model import Entity, Error, Service


class Execution[T: Entity]:
    def __init__(self, subject_type: type[T]) -> None:
        self.subject = Entity.__new__(subject_type)
        self._greenlets: dict[int, greenlet] = {}
        self._initiators: dict[greenlet, InitiatorMessage] = {}
        self._errors = WeakKeyDictionary[Error, _ErrorArguments]()
        self._context: list[ContextMessage] = []
        self._offset = 0

    @property
    def context(self) -> list[ContextMessage]:
        return self._context.copy()

    def complete(self, messages: Iterable[ContextMessage]) -> list[OutputMessage]:
        input_deque = deque(messages)
        for cached_message in self._context:
            if not input_deque or input_deque.popleft() != cached_message:
                raise NotImplementedError("Cache miss")
            self._offset = cached_message.offset + 1

        output_deque = deque[OutputMessage]()
        while input_deque:
            event = input_deque.popleft()

            # Initial request received
            if isinstance(event, CreateEntityRequestReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = greenlet(getattr(type(self.subject), "__init__"))
                self._initiators[execution] = event
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            self.subject,
                            *event.args,
                            **event.kwargs,
                        ),
                    )
                )
            elif isinstance(event, EntityMethodRequestReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = greenlet(getattr(type(self.subject), event.method_name))
                self._initiators[execution] = event
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            self.subject,
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

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.entity,
                        ),
                    )
                )
            elif isinstance(event, EntityMethodResponseReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.response,
                        ),
                    )
                )
            elif isinstance(event, ServiceMethodResponseReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            event.response,
                        ),
                    )
                )
            elif isinstance(event, EntityMethodErrorReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                error = event.error_type(*event.args, **event.kwargs)
                self._errors[error] = _ErrorArguments(
                    event.error_type,
                    event.args,
                    event.kwargs,
                )

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            event.error_type,
                            error,
                        ),
                    )
                )
            elif isinstance(event, ServiceMethodErrorReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            type(event.exception),
                            event.exception,
                        ),
                    )
                )
            elif isinstance(event, CreateEntityErrorReceived):
                if event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                self._offset = event.offset + 1

                error = event.error_type(*event.args, **event.kwargs)
                self._errors[error] = _ErrorArguments(
                    event.error_type,
                    event.args,
                    event.kwargs,
                )

                execution = self._greenlets.pop(event.request_offset)
                self._context.append(event)
                output_deque.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            event.error_type,
                            error,
                        ),
                    )
                )

            # Request sent
            elif isinstance(event, CreateEntityRequestSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)
            elif isinstance(event, EntityMethodRequestSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)
            elif isinstance(event, ServiceMethodRequestSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)

            # Response sent
            elif isinstance(event, CreateEntityResponseSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)
            elif isinstance(event, CreateEntityErrorSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)
            elif isinstance(event, EntityMethodResponseSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)
            elif isinstance(event, EntityMethodErrorSent):
                if event != output_deque.popleft():
                    raise NotImplementedError("Inconsistent execution context")
                self._context.append(event)

            # State changed
            elif isinstance(event, EntityStateChanged):
                if output_deque:
                    if event != output_deque.popleft():
                        raise NotImplementedError("Inconsistent execution context")
                elif event.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")
                getattr(self.subject, "__setstate__")(event.state)
                self._context.append(event)
                self._offset = event.offset + 1

            else:
                assert_never(event)

        self._context.extend(output_deque)
        return list(output_deque)

    def cleanup(self) -> list[ContextMessage]:
        processed_offsets = set[int]()

        # Gather processed requests, their responses
        # and requests sent during processing
        for message in reversed(self._context):
            if isinstance(message, RESPONSE_SENT):
                processed_offsets.add(message.request_offset)
                processed_offsets.add(message.offset)
            elif (
                isinstance(message, REQUEST_SENT)
                and message.trace_offset in processed_offsets
            ):
                processed_offsets.add(message.offset)

        # Gather responses received within processed requests
        for message in self._context:
            if (
                isinstance(message, RESPONSE_RECEIVED)
                and message.request_offset in processed_offsets
            ):
                processed_offsets.add(message.offset)

        # Gather consecutive state changed
        last_unprocessed: ContextMessage | None = None
        for message in self._context:
            if message.offset in processed_offsets:
                continue
            if (
                last_unprocessed is not None
                and isinstance(message, EntityStateChanged)
                and isinstance(last_unprocessed, EntityStateChanged)
            ):
                processed_offsets.add(last_unprocessed.offset)
            last_unprocessed = message

        processed: list[ContextMessage] = []
        unprocessed: list[ContextMessage] = []
        for message in self._context:
            if message.offset in processed_offsets:
                processed.append(message)
            else:
                unprocessed.append(message)

        self._context = unprocessed
        return processed

    def _continue(
        self,
        execution: greenlet,
        switch_to_execution: Callable[[], Any],
    ) -> list[OutputMessage]:
        initiator = self._initiators[execution]

        try:
            with Execution._intercept_interaction(self, self.subject, initiator.offset):
                interception = switch_to_execution()
        except Error as ex:
            try:
                error_arguments = self._errors[ex]
            except KeyError:
                raise ex

            if isinstance(initiator, CreateEntityRequestReceived):
                return [
                    CreateEntityErrorSent(
                        offset=self._next_offset(),
                        request_offset=initiator.offset,
                        error_type=error_arguments.error_type,
                        args=error_arguments.args,
                        kwargs=error_arguments.kwargs,
                    )
                ]
            elif isinstance(initiator, EntityMethodRequestReceived):
                return [
                    EntityMethodErrorSent(
                        offset=self._next_offset(),
                        request_offset=initiator.offset,
                        error_type=error_arguments.error_type,
                        args=error_arguments.args,
                        kwargs=error_arguments.kwargs,
                    ),
                    EntityStateChanged(
                        offset=self._next_offset(),
                        state=self.subject.__getstate__(),
                    ),
                ]
            else:
                assert_never(initiator)

        if not execution.dead:
            self._greenlets[interception.offset] = execution
            return [interception]

        del self._initiators[execution]
        if isinstance(initiator, CreateEntityRequestReceived):
            return [
                CreateEntityResponseSent(
                    offset=self._next_offset(),
                    request_offset=initiator.offset,
                ),
                EntityStateChanged(
                    offset=self._next_offset(),
                    state=self.subject.__getstate__(),
                ),
            ]
        elif isinstance(initiator, EntityMethodRequestReceived):
            return [
                EntityMethodResponseSent(
                    offset=self._next_offset(),
                    request_offset=initiator.offset,
                    response=interception,
                ),
                EntityStateChanged(
                    offset=self._next_offset(),
                    state=self.subject.__getstate__(),
                ),
            ]
        else:
            assert_never(initiator)

    @contextmanager
    def _intercept_interaction(
        self,
        subject: Entity,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        with (
            Execution._intercept_create_entity(self, trace_offset),
            Execution._intercept_send_entity_request(self, subject, trace_offset),
            Execution._intercept_send_service_request(self, subject, trace_offset),
            Execution._intercept_entity_error(self),
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
                    EntityMethodRequestSent(
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
                    ServiceMethodRequestSent(
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
            self._errors[error] = _ErrorArguments(cls, args, kwargs)
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
