from dataclasses import dataclass

from runa import Entity, Runa
from runa.execution import (
    InitializeReceived,
    InitializeHandled,
    StateChanged,
)


@dataclass
class UserState:
    name: str


class User(Entity[UserState]):
    def __init__(self, name: str) -> None:
        self.name = name

    def __getstate__(self) -> UserState:
        return UserState(self.name)

    def __setstate__(self, state: UserState) -> None:
        self.name = state.name


def test_execute_initialize_received() -> None:
    result = Runa(User).execute(
        context=[
            InitializeReceived(
                id="request-1",
                args=("Yura",),
                kwargs={},
            ),
        ],
    )
    assert isinstance(result.entity, User)
    assert result.entity.name == "Yura"
    assert result.context == [
        InitializeReceived(
            id="request-1",
            args=("Yura",),
            kwargs={},
        ),
        InitializeHandled(
            id=result.context[1].id,
            request_id="request-1",
        ),
        StateChanged(
            id=result.context[2].id,
            state=UserState("Yura"),
        ),
    ]


def test_execute_state_changed() -> None:
    result = Runa(User).execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yura"),
            ),
        ],
    )
    assert isinstance(result.entity, User)
    assert result.entity.name == "Yura"
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yura"),
        ),
    ]
