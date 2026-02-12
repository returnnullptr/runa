from dataclasses import dataclass
from typing import Any
from uuid import uuid7

from greenlet import greenlet

from runa import Entity


@dataclass(kw_only=True, frozen=True)
class InitializeReceived:
    id: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass(kw_only=True, frozen=True)
class InitializeHandled:
    id: str
    request_id: str


@dataclass(kw_only=True, frozen=True)
class StateChanged:
    id: str
    state: Any


ExecutionContext = list[InitializeReceived | InitializeHandled | StateChanged]


class ExecutionResult:
    def __init__(
        self,
        entity: Entity[Any],
        context: ExecutionContext,
    ) -> None:
        self.entity = entity
        self.context = context


class Runa:
    def __init__(self, entity_type: type[Entity[Any]]) -> None:
        self.entity_type = entity_type

    def execute(
        self,
        context: ExecutionContext,
    ) -> ExecutionResult:
        entity = Entity.__new__(self.entity_type)
        result = ExecutionResult(entity, [])

        assert len(context) == 1
        event = context[0]

        if isinstance(event, InitializeReceived):
            execution = greenlet(getattr(self.entity_type, "__init__"))
            execution.switch(entity, *event.args, *event.kwargs)

            if not execution.dead:
                raise NotImplementedError

            result.context.append(event)

            result.context.append(
                InitializeHandled(
                    id=uuid7().hex,
                    request_id=event.id,
                )
            )
            result.context.append(
                StateChanged(
                    id=uuid7().hex,
                    state=entity.__getstate__(),
                )
            )
        elif isinstance(event, StateChanged):
            result.context.append(event)
            entity.__setstate__(event.state)

        return result
