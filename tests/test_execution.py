from abc import abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from runa import Entity, Runa, Service
from runa.execution import (
    InitializeRequestReceived,
    InitializeResponseSent,
    StateChanged,
    RequestReceived,
    ResponseSent,
    CreateEntityRequestSent,
    CreateEntityResponseReceived,
    EntityRequestSent,
    EntityResponseReceived,
    ServiceRequestSent,
    ServiceResponseReceived,
)


class Species(StrEnum):
    CAT = "Cat"
    DOG = "Dog"


class PetNameGenerator(Service):
    @abstractmethod
    def generate_name(self, species: Species) -> str: ...


@dataclass
class UserState:
    name: str
    pets: list[Pet]


class User(Entity):
    pet_name_generator: PetNameGenerator

    def __init__(self, name: str) -> None:
        self.name = name
        self.pets: list[Pet] = []

    def __getstate__(self) -> UserState:
        return UserState(self.name, self.pets.copy())

    def __setstate__(self, state: UserState) -> None:
        self.name = state.name
        self.pets = state.pets.copy()

    def change_name(self, name: str) -> str:
        self.name = name
        return "Sure!"

    def add_pet(self, name: str) -> None:
        self.pets.append(Pet(name, owner=self))

    def rename_pet(self, pet: Pet, new_name: str) -> None:
        pet.change_name(self, new_name)

    def come_up_pet_name(self, species: Species) -> str:
        return self.pet_name_generator.generate_name(species)


@dataclass
class PetState:
    name: str


class Pet(Entity):
    def __init__(self, name: str, owner: User) -> None:
        self.name = name
        self.owner = owner

    def __getstate__(self) -> PetState:
        return PetState(self.name)

    def __setstate__(self, state: PetState) -> None:
        self.name = state.name

    def change_name(self, user: User, new_name: str) -> bool:
        if self.owner is user:
            self.name = new_name
            return True
        return False


def test_initialize_request_received() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            InitializeRequestReceived(
                id="request-1",
                args=("Yura",),
                kwargs={},
            ),
        ],
    )
    assert user.entity.name == "Yura"
    assert result.context == [
        InitializeRequestReceived(
            id="request-1",
            args=("Yura",),
            kwargs={},
        ),
        InitializeResponseSent(
            id=result.context[1].id,
            request_id="request-1",
        ),
        StateChanged(
            id=result.context[2].id,
            state=UserState("Yura", []),
        ),
    ]


def test_state_changed() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yura", []),
            ),
        ],
    )
    assert user.entity.name == "Yura"
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yura", []),
        ),
    ]


def test_request_received() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yura", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="change_name",
                args=("Yuriy",),
                kwargs={},
            ),
        ],
    )
    assert user.entity.name == "Yuriy"
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yura", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="change_name",
            args=("Yuriy",),
            kwargs={},
        ),
        ResponseSent(
            id=result.context[2].id,
            request_id="request-1",
            response="Sure!",
        ),
        StateChanged(
            id=result.context[3].id,
            state=UserState("Yuriy", []),
        ),
    ]


def test_create_entity_request_sent() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="add_pet",
                args=(),
                kwargs={"name": "Stitch"},
            ),
        ],
    )
    assert not user.entity.pets
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            id=result.context[2].id,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": user.entity},
        ),
    ]


def test_create_entity_response_received() -> None:
    user = Runa(User)
    pet = Pet("Stitch", owner=user.entity)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="add_pet",
                args=(),
                kwargs={"name": "Stitch"},
            ),
            CreateEntityRequestSent(
                id="create-entity-1",
                entity_type=Pet,
                args=("Stitch",),
                kwargs={"owner": user.entity},
            ),
            CreateEntityResponseReceived(
                id="entity-created-1",
                request_id="create-entity-1",
                entity=pet,
            ),
        ],
    )
    assert user.entity.pets == [pet]
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            id="create-entity-1",
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": user.entity},
        ),
        CreateEntityResponseReceived(
            id="entity-created-1",
            request_id="create-entity-1",
            entity=pet,
        ),
        ResponseSent(
            id=result.context[4].id,
            request_id="request-1",
            response=None,
        ),
        StateChanged(
            id=result.context[5].id,
            state=UserState("Yuriy", [pet]),
        ),
    ]


def test_entity_request_sent() -> None:
    user = Runa(User)
    pet = Pet("my_cat", owner=user.entity)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", [pet]),
            ),
            RequestReceived(
                id="request-1",
                method_name="rename_pet",
                args=(pet,),
                kwargs={"new_name": "Stitch"},
            ),
        ],
    )
    assert pet.name == "my_cat"
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", [pet]),
        ),
        RequestReceived(
            id="request-1",
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Stitch"},
        ),
        EntityRequestSent(
            id=result.context[2].id,
            trace_id="request-1",
            receiver=pet,
            method_name="change_name",
            args=(user.entity, "Stitch"),
            kwargs={},
        ),
    ]


def test_entity_response_received() -> None:
    user = Runa(User)
    pet = Pet("Stitch", owner=user.entity)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", [pet]),
            ),
            RequestReceived(
                id="request-1",
                method_name="rename_pet",
                args=(pet,),
                kwargs={"new_name": "Stitch"},
            ),
            EntityRequestSent(
                id="request-2",
                trace_id="request-1",
                receiver=pet,
                method_name="change_name",
                args=(user.entity, "Stitch"),
                kwargs={},
            ),
            EntityResponseReceived(
                id="response-1",
                request_id="request-2",
                response=True,
            ),
        ],
    )
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", [pet]),
        ),
        RequestReceived(
            id="request-1",
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Stitch"},
        ),
        EntityRequestSent(
            id="request-2",
            trace_id="request-1",
            receiver=pet,
            method_name="change_name",
            args=(user.entity, "Stitch"),
            kwargs={},
        ),
        EntityResponseReceived(
            id="response-1",
            request_id="request-2",
            response=True,
        ),
        ResponseSent(
            id=result.context[4].id,
            request_id="request-1",
            response=None,
        ),
        StateChanged(
            id=result.context[5].id,
            state=UserState("Yuriy", [pet]),
        ),
    ]


def test_service_request_published() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="come_up_pet_name",
                args=(Species.CAT,),
                kwargs={},
            ),
        ],
    )
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceRequestSent(
            id=result.context[2].id,
            trace_id="request-1",
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
    ]


def test_service_response_received() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="come_up_pet_name",
                args=(Species.CAT,),
                kwargs={},
            ),
            ServiceRequestSent(
                id="request-2",
                trace_id="request-1",
                service_type=PetNameGenerator,
                method_name="generate_name",
                args=(Species.CAT,),
                kwargs={},
            ),
            ServiceResponseReceived(
                id="response-1",
                request_id="request-2",
                response="Stitch",
            ),
        ],
    )
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceRequestSent(
            id="request-2",
            trace_id="request-1",
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceResponseReceived(
            id="response-1",
            request_id="request-2",
            response="Stitch",
        ),
        ResponseSent(
            id=result.context[4].id,
            request_id="request-1",
            response="Stitch",
        ),
        StateChanged(
            id=result.context[5].id,
            state=UserState("Yuriy", []),
        ),
    ]


def test_context_not_changed() -> None:
    user = Runa(User)
    result = user.execute(
        context=[
            StateChanged(
                id="state-changed-1",
                state=UserState("Yuriy", []),
            ),
            RequestReceived(
                id="request-1",
                method_name="come_up_pet_name",
                args=(Species.CAT,),
                kwargs={},
            ),
            ServiceRequestSent(
                id="request-2",
                trace_id="request-1",
                service_type=PetNameGenerator,
                method_name="generate_name",
                args=(Species.CAT,),
                kwargs={},
            ),
            ServiceResponseReceived(
                id="response-1",
                request_id="request-2",
                response="Stitch",
            ),
            ResponseSent(
                id="response-2",
                request_id="request-1",
                response="Stitch",
            ),
            StateChanged(
                id="state-changed-2",
                state=UserState("Yuriy", []),
            ),
        ],
    )
    assert result.context == [
        StateChanged(
            id="state-changed-1",
            state=UserState("Yuriy", []),
        ),
        RequestReceived(
            id="request-1",
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceRequestSent(
            id="request-2",
            trace_id="request-1",
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceResponseReceived(
            id="response-1",
            request_id="request-2",
            response="Stitch",
        ),
        ResponseSent(
            id="response-2",
            request_id="request-1",
            response="Stitch",
        ),
        StateChanged(
            id="state-changed-2",
            state=UserState("Yuriy", []),
        ),
    ]
