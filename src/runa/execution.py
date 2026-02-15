import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable, assert_never
from weakref import WeakKeyDictionary

from greenlet import greenlet

from runa.context import (
    ContextMessage,
    CreateEntityErrorSent,
    CreateEntityRequestReceived,
    CreateEntityRequestSent,
    CreateEntityResponseSent,
    EntityMethodErrorSent,
    EntityMethodRequestReceived,
    EntityMethodRequestSent,
    EntityMethodResponseSent,
    EntityStateChanged,
    InitiatorMessage,
    OutputMessage,
    ServiceMethodErrorReceived,
    ServiceMethodRequestSent,
    RESPONSE_SENT,
    REQUEST_SENT,
    RESPONSE_RECEIVED,
    ERROR_RECEIVED,
    REQUEST_RECEIVED,
)
from runa.model import Entity, Error, Service


class Execution[Subject: Entity]:
    def __init__(self, subject_type: type[Subject]) -> None:
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
        input_iterator = iter(messages)
        for message in self._context:
            # TODO: Reset execution state and raise custom error
            try:
                if message != next(input_iterator):
                    raise NotImplementedError("Cache miss")
            except StopIteration:
                raise NotImplementedError("Cache miss")

            self._offset = message.offset + 1

        # TODO: Wrap with try-except and reset execution state in case of error
        output_messages = deque[OutputMessage]()
        while True:
            try:
                message = next(input_iterator)
            except StopIteration:
                break

            if isinstance(message, REQUEST_RECEIVED):
                # TODO: Reset execution state and raise custom error
                if message.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")

                if isinstance(message, CreateEntityRequestReceived):
                    method = getattr(type(self.subject), "__init__")
                elif isinstance(message, EntityMethodRequestReceived):
                    method = getattr(type(self.subject), message.method_name)
                else:
                    assert_never(message)  # pragma: no cover

                execution = greenlet(method)
                self._initiators[execution] = message

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            self.subject,
                            *message.args,
                            **message.kwargs,
                        ),
                    )
                )
            elif isinstance(message, RESPONSE_RECEIVED):
                # TODO: Reset execution state and raise custom error
                if message.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")

                execution = self._greenlets.pop(message.request_offset)

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.switch,
                            message.response,
                        ),
                    )
                )
            elif isinstance(message, ERROR_RECEIVED):
                # TODO: Reset execution state and raise custom error
                if message.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")

                if isinstance(message, ServiceMethodErrorReceived):
                    exception = message.exception
                else:
                    exception = message.error_type(*message.args, **message.kwargs)
                    self._errors[exception] = _ErrorArguments(
                        message.error_type,
                        message.args,
                        message.kwargs,
                    )

                execution = self._greenlets.pop(message.request_offset)

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        execution,
                        functools.partial(
                            execution.throw,
                            type(exception),
                            exception,
                        ),
                    )
                )
            elif (
                isinstance(message, REQUEST_SENT)  #
                or isinstance(message, RESPONSE_SENT)
            ):
                # TODO: Reset execution state and raise custom error
                if message != output_messages.popleft():
                    raise NotImplementedError("Inconsistent execution context")

                self._context.append(message)
            elif isinstance(message, EntityStateChanged):
                # TODO: Reset execution state and raise custom error
                if output_messages:
                    if message != output_messages.popleft():
                        raise NotImplementedError("Inconsistent execution context")
                elif message.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")

                getattr(self.subject, "__setstate__")(message.state)
                self._context.append(message)
                self._offset = message.offset + 1
            else:
                assert_never(message)  # pragma: no cover

        self._context.extend(output_messages)
        return list(output_messages)

    def cleanup(self) -> list[ContextMessage]:
        processed_offsets = set[int]()

        # Gather responses sent, their requests and requests sent during processing
        for message in reversed(self._context):
            if isinstance(message, RESPONSE_SENT):
                processed_offsets.add(message.request_offset)
                processed_offsets.add(message.offset)
            elif (
                isinstance(message, REQUEST_SENT)
                and message.trace_offset in processed_offsets
            ):
                processed_offsets.add(message.offset)

        # Gather responses and errors received within processed requests
        for message in self._context:
            if (
                isinstance(message, RESPONSE_RECEIVED)  #
                or isinstance(message, ERROR_RECEIVED)
            ) and message.request_offset in processed_offsets:
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
            with Execution._intercept_interaction(self, initiator.offset):
                interception = switch_to_execution()
        except Error as ex:
            error_arguments = self._errors[ex]

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
                assert_never(initiator)  # pragma: no cover

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
            assert_never(initiator)  # pragma: no cover

    @contextmanager
    def _intercept_interaction(
        self,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        with (
            Execution._intercept_create_entity(self, trace_offset),
            Execution._intercept_send_entity_request(self, trace_offset),
            Execution._intercept_send_service_request(self, trace_offset),
            Execution._intercept_entity_error(self),
            # TODO: Protect entity state from modifying by another entity
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
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def getattribute(entity: Entity, name: str) -> Any:
            if entity is self.subject:
                return original_getattribute(entity, name)

            if name.startswith("_"):
                # TODO: Test this behavior
                raise AttributeError("Entity state is private")

            original_method = getattr(type(entity), name)
            if not inspect.isfunction(original_method):
                # TODO: Test this behavior
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
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def getattribute(service: Service, name: str) -> Any:
            original_method = getattr(type(service), name)

            if name.startswith("_"):
                # TODO: Test this behavior
                raise AttributeError("Service state is private")

            if not inspect.isfunction(original_method):
                # TODO: Test this behavior
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
        for attr_name, annotation in inspect.get_annotations(
            type(self.subject)
        ).items():
            if issubclass(annotation, Service):
                proxy = _ServiceProxy()
                proxy.__class__ = annotation
                proxies.append((attr_name, proxy))

        for attr_name, service_proxy in proxies:
            setattr(self.subject, attr_name, service_proxy)

        original_getattribute = Service.__getattribute__
        setattr(Service, "__getattribute__", getattribute)
        try:
            yield
        finally:
            for attr_name, _ in proxies:
                delattr(self.subject, attr_name)
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
