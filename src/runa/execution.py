import functools
import inspect
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, Iterable, assert_never
from weakref import WeakKeyDictionary

from greenlet import greenlet

from runa.context import (
    ERROR_RECEIVED,
    ERROR_SENT,
    REQUEST_RECEIVED,
    REQUEST_SENT,
    RESPONSE_RECEIVED,
    RESPONSE_SENT,
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
        input_messages = iter(messages)
        for message in self._context:
            # TODO: Reset execution state and raise custom error
            try:
                if message != next(input_messages):
                    raise NotImplementedError("Cache miss")
            except StopIteration:
                raise NotImplementedError("Cache miss")

            self._offset = message.offset + 1

        # TODO: Wrap with try-except and reset execution state in case of error
        output_messages = deque[OutputMessage]()
        for message in input_messages:
            if isinstance(message, REQUEST_RECEIVED):
                # TODO: Reset execution state and raise custom error
                if message.offset < self._offset:
                    raise NotImplementedError("Unordered offsets")

                if isinstance(message, CreateEntityRequestReceived):
                    method = getattr(type(self.subject), "__init__")
                elif isinstance(message, EntityMethodRequestReceived):
                    if message.method not in vars(type(self.subject)).values():
                        # TODO: Test this behavior
                        raise NotImplementedError("Undefined entity method")
                    method = message.method
                else:
                    assert_never(message)  # pragma: no cover

                method_greenlet = greenlet(method)
                self._initiators[method_greenlet] = message

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        method_greenlet,
                        functools.partial(
                            method_greenlet.switch,
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

                method_greenlet = self._greenlets.pop(message.request_offset)

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        method_greenlet,
                        functools.partial(
                            method_greenlet.switch,
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

                method_greenlet = self._greenlets.pop(message.request_offset)

                self._context.append(message)
                self._offset = message.offset + 1
                output_messages.extend(
                    self._continue(
                        method_greenlet,
                        functools.partial(
                            method_greenlet.throw,
                            type(exception),
                            exception,
                        ),
                    )
                )
            elif (
                isinstance(message, REQUEST_SENT)
                or isinstance(message, RESPONSE_SENT)
                or isinstance(message, ERROR_SENT)
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

        # Gather responses and errors sent, their initiator messages and requests sent during processing
        for message in reversed(self._context):
            if isinstance(message, RESPONSE_SENT) or isinstance(message, ERROR_SENT):
                processed_offsets.add(message.request_offset)
                processed_offsets.add(message.offset)
            elif (
                isinstance(message, REQUEST_SENT)
                and message.trace_offset in processed_offsets
            ):
                processed_offsets.add(message.offset)

        # Gather responses and errors received within processed messages
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
        method_greenlet: greenlet,
        switch_to_greenlet: Callable[[], Any],
    ) -> list[OutputMessage]:
        initiator = self._initiators[method_greenlet]

        try:
            with Execution._intercept_interaction(self, initiator.offset):
                output_message_or_result = switch_to_greenlet()
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

        if not method_greenlet.dead:
            output_message = output_message_or_result
            self._greenlets[output_message.offset] = method_greenlet
            return [output_message]

        result = output_message_or_result
        del self._initiators[method_greenlet]
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
                    response=result,
                ),
                EntityStateChanged(
                    offset=self._next_offset(),
                    state=self.subject.__getstate__(),
                ),
            ]
        else:
            assert_never(initiator)  # pragma: no cover

    def _next_offset(self) -> int:
        offset = self._offset
        self._offset += 1
        return offset

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
            Execution._protect_entity_private_state(self),
        ):
            yield

    @contextmanager
    def _intercept_create_entity(
        self,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def patched_new(cls: type[Entity], *args: Any, **kwargs: Any) -> Entity:
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
            def temporary_patched_init(*_: Any, **__: Any) -> None:
                setattr(cls, "__init__", not_patched_init)

            not_patched_init = getattr(cls, "__init__")
            setattr(cls, "__init__", temporary_patched_init)
            return entity

        not_patched_new = Entity.__new__
        setattr(Entity, "__new__", patched_new)
        try:
            yield
        finally:
            setattr(Entity, "__new__", not_patched_new)

    @contextmanager
    def _intercept_send_entity_request(
        self,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def patched_getattribute(entity: Entity, name: str) -> Any:
            if entity is self.subject:
                return not_patched_getattribute(entity, name)

            if name.startswith("_"):
                raise AttributeError("Entity state is private")

            try:
                original_method = getattr(type(entity), name)
            except AttributeError:
                raise AttributeError("Entity state is private")

            if not inspect.isfunction(original_method):
                # TODO: Test this behavior
                raise AttributeError("Entity state is private")

            @functools.wraps(original_method)
            def patched_method(_: Any, /, *args: Any, **kwargs: Any) -> Any:
                return main_greenlet.switch(
                    EntityMethodRequestSent(
                        offset=self._next_offset(),
                        trace_offset=trace_offset,
                        receiver=entity,
                        method=original_method,
                        args=args,
                        kwargs=kwargs,
                    )
                )

            return functools.partial(patched_method, entity)

        not_patched_getattribute = Entity.__getattribute__
        setattr(Entity, "__getattribute__", patched_getattribute)
        try:
            yield
        finally:
            setattr(Entity, "__getattribute__", not_patched_getattribute)

    @contextmanager
    def _protect_entity_private_state(self) -> Generator[None, None, None]:
        def patched_setattr(entity: Entity, name: str, value: Any) -> None:
            if entity is self.subject:
                return not_patched_setattr(entity, name, value)

            raise AttributeError("Entity state is private")

        not_patched_setattr = Entity.__setattr__
        setattr(Entity, "__setattr__", patched_setattr)
        try:
            yield
        finally:
            setattr(Entity, "__setattr__", not_patched_setattr)

    @contextmanager
    def _intercept_send_service_request(
        self,
        trace_offset: int,
    ) -> Generator[None, None, None]:
        main_greenlet = greenlet.getcurrent()

        def patched_getattribute(service: Service, name: str) -> Any:
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
                        method=original_method,
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

        not_patched_getattribute = Service.__getattribute__
        setattr(Service, "__getattribute__", patched_getattribute)
        try:
            yield
        finally:
            for attr_name, _ in proxies:
                delattr(self.subject, attr_name)
            setattr(Entity, "__getattribute__", not_patched_getattribute)

    @contextmanager
    def _intercept_entity_error(self) -> Generator[None, None, None]:
        def patched_new(cls: type[Error], *args: Any, **kwargs: Any) -> Error:
            error = not_patched_new(cls, *args, **kwargs)
            self._errors[error] = _ErrorArguments(cls, args, kwargs)
            return error

        not_patched_new = Error.__new__
        setattr(Error, "__new__", patched_new)
        try:
            yield
        finally:
            setattr(Error, "__new__", not_patched_new)


class _ServiceProxy:
    pass


@dataclass
class _ErrorArguments:
    error_type: type[Error]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
