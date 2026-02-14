from abc import abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from runa import Entity, Runa, Service, Error
from runa.execution import (
    CreateEntityRequestReceived,
    CreateEntityResponseSent,
    StateChanged,
    EntityRequestReceived,
    EntityResponseSent,
    CreateEntityRequestSent,
    CreateEntityResponseReceived,
    EntityRequestSent,
    EntityResponseReceived,
    ServiceRequestSent,
    ServiceResponseReceived,
    EntityErrorReceived,
    EntityErrorSent,
    ServiceErrorReceived,
    ContextMessage,
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

    def change_name(self, user: User, new_name: str) -> None:
        if self.owner is not user:
            raise UserIsNotPetOwnerError(user, self)
        self.name = new_name


class UserIsNotPetOwnerError(Error):
    def __init__(self, user: User, pet: Pet) -> None:
        self.runa = user
        self.pet = pet


@dataclass
class ProjectState:
    readme: Readme
    tests: str | None
    code: str | None


class CodeGenerator(Service):
    @abstractmethod
    def generate_tests(self) -> str: ...

    @abstractmethod
    def generate_code(self, tests: str) -> str: ...


class CodeGeneratingFailed(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


class Project(Entity):
    code_generator: CodeGenerator

    def __init__(self, description: str) -> None:
        self.readme = Readme(description)
        self.tests: str | None = None
        self.code: str | None = None

    def __getstate__(self) -> ProjectState:
        return ProjectState(self.readme, self.tests, self.code)

    def __setstate__(self, state: ProjectState) -> None:
        self.readme = state.readme
        self.tests = state.tests
        self.code = state.code

    def implement(self) -> None:
        try:
            tests = self.code_generator.generate_tests()
            self.code_generator.generate_code(tests)
        except CodeGeneratingFailed as ex:
            raise ProjectImplementationError(ex.message)


class ProjectImplementationError(Error):
    def __init__(self, message: str) -> None:
        self.message = message


class Readme(Entity):
    def __init__(self, content: str) -> None:
        self.content = content

    def __getstate__(self) -> str:
        return self.content

    def __setstate__(self, content: str) -> None:
        self.content = content


def test_create_entity_request_received() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Yura",),
            kwargs={},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        CreateEntityResponseSent(
            offset=1,
            request_offset=0,
        ),
        StateChanged(
            offset=2,
            state=UserState("Yura", []),
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert runa.entity.name == "Yura"


def test_state_changed() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yura", []),
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == []
    assert runa.context == input_messages + output_messages
    assert runa.entity.name == "Yura"


def test_entity_request_received() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yura", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="change_name",
            args=("Yuriy",),
            kwargs={},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=2,
            request_offset=1,
            response="Sure!",
        ),
        StateChanged(
            offset=3,
            state=UserState("Yuriy", []),
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert runa.entity.name == "Yuriy"


def test_create_entity_request_sent() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert not runa.entity.pets


def test_create_entity_response_received() -> None:
    runa = Runa(User)
    pet = Pet("Stitch", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
        CreateEntityResponseReceived(
            offset=3,
            request_offset=2,
            entity=pet,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=4,
            request_offset=1,
            response=None,
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", [pet]),
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert runa.entity.pets == [pet]


def test_entity_request_sent() -> None:
    runa = Runa(User)
    pet = Pet("my_cat", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", [pet]),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Stitch"},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityRequestSent(
            offset=2,
            trace_offset=1,
            receiver=pet,
            method_name="change_name",
            args=(runa.entity, "Stitch"),
            kwargs={},
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert pet.name == "my_cat"


def test_entity_response_received() -> None:
    runa = Runa(User)
    pet = Pet("Stitch", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", [pet]),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Stitch"},
        ),
        EntityRequestSent(
            offset=2,
            trace_offset=1,
            receiver=pet,
            method_name="change_name",
            args=(runa.entity, "Stitch"),
            kwargs={},
        ),
        EntityResponseReceived(
            offset=3,
            request_offset=2,
            response=None,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=4,
            request_offset=1,
            response=None,
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", [pet]),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_service_request_sent() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        ServiceRequestSent(
            offset=2,
            trace_offset=1,
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_service_response_received() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceRequestSent(
            offset=2,
            trace_offset=1,
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceResponseReceived(
            offset=3,
            request_offset=2,
            response="Stitch",
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=4,
            request_offset=1,
            response="Stitch",
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", []),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_context_not_changed() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="come_up_pet_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceRequestSent(
            offset=2,
            trace_offset=1,
            service_type=PetNameGenerator,
            method_name="generate_name",
            args=(Species.CAT,),
            kwargs={},
        ),
        ServiceResponseReceived(
            offset=3,
            request_offset=2,
            response="Stitch",
        ),
        EntityResponseSent(
            offset=4,
            request_offset=1,
            response="Stitch",
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", []),
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == []
    assert runa.context == input_messages + output_messages


def test_create_entity_request_received_create_entity_request_sent() -> None:
    runa = Runa(Project)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Research project",),
            kwargs={},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        CreateEntityRequestSent(
            offset=1,
            trace_offset=0,
            entity_type=Readme,
            args=("Research project",),
            kwargs={},
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_create_entity_response_received_create_entity_response_sent() -> None:
    runa = Runa(Project)
    readme = Readme("Research project")
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Research project",),
            kwargs={},
        ),
        CreateEntityRequestSent(
            offset=1,
            trace_offset=0,
            entity_type=Readme,
            args=("Research project",),
            kwargs={},
        ),
        CreateEntityResponseReceived(
            offset=2,
            request_offset=1,
            entity=readme,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        CreateEntityResponseSent(
            offset=3,
            request_offset=0,
        ),
        StateChanged(
            offset=4,
            state=ProjectState(readme, None, None),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_request_sequence() -> None:
    runa = Runa(Project)
    readme = Readme("Research project")
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=ProjectState(readme, None, None),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="implement",
            args=(),
            kwargs={},
        ),
        ServiceRequestSent(
            offset=2,
            trace_offset=1,
            service_type=CodeGenerator,
            method_name="generate_tests",
            args=(),
            kwargs={},
        ),
        ServiceResponseReceived(
            offset=3,
            request_offset=2,
            response="def test_nothing() -> None: assert True",
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        ServiceRequestSent(
            offset=4,
            trace_offset=1,
            service_type=CodeGenerator,
            method_name="generate_code",
            args=("def test_nothing() -> None: assert True",),
            kwargs={},
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_execution_context_cached() -> None:
    runa = Runa(User)
    pet = Pet("Stitch", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
        CreateEntityResponseReceived(
            offset=3,
            request_offset=2,
            entity=pet,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=4,
            request_offset=1,
            response=None,
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", [pet]),
        ),
    ]
    assert runa.context == input_messages + output_messages
    assert runa.execute(runa.context) == []
    assert runa.context == input_messages + output_messages


def test_entity_error_received() -> None:
    runa = Runa(User)
    pet = Pet("Stitch", owner=User("Kate"))
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Helicopter"},
        ),
        EntityRequestSent(
            offset=2,
            trace_offset=1,
            receiver=pet,
            method_name="change_name",
            args=(runa.entity, "Helicopter"),
            kwargs={},
        ),
        EntityErrorReceived(
            offset=3,
            request_offset=2,
            error_type=UserIsNotPetOwnerError,
            args=(runa.entity, pet),
            kwargs={},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityErrorSent(
            offset=4,
            request_offset=1,
            error_type=UserIsNotPetOwnerError,
            args=(runa.entity, pet),
            kwargs={},
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", []),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_entity_error_sent_next_request_received() -> None:
    runa = Runa(User)
    pet = Pet("Stitch", owner=User("Kate"))
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="rename_pet",
            args=(pet,),
            kwargs={"new_name": "Helicopter"},
        ),
        EntityRequestSent(
            offset=2,
            trace_offset=1,
            receiver=pet,
            method_name="change_name",
            args=(runa.entity, "Helicopter"),
            kwargs={},
        ),
        EntityErrorReceived(
            offset=3,
            request_offset=2,
            error_type=UserIsNotPetOwnerError,
            args=(runa.entity, pet),
            kwargs={},
        ),
        EntityErrorSent(
            offset=4,
            request_offset=1,
            error_type=UserIsNotPetOwnerError,
            args=(runa.entity, pet),
            kwargs={},
        ),
        StateChanged(
            offset=5,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=6,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Helicopter"},
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        CreateEntityRequestSent(
            offset=7,
            trace_offset=6,
            entity_type=Pet,
            args=("Helicopter",),
            kwargs={"owner": runa.entity},
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_service_error_received() -> None:
    runa = Runa(Project)
    readme = Readme("Research project")
    service_exception = CodeGeneratingFailed("Not enough description")
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=ProjectState(readme, None, None),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="implement",
            args=(),
            kwargs={},
        ),
        ServiceRequestSent(
            offset=2,
            trace_offset=1,
            service_type=CodeGenerator,
            method_name="generate_tests",
            args=(),
            kwargs={},
        ),
        ServiceErrorReceived(
            offset=3,
            request_offset=2,
            exception=service_exception,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityErrorSent(
            offset=4,
            request_offset=1,
            error_type=ProjectImplementationError,
            args=("Not enough description",),
            kwargs={},
        ),
        StateChanged(
            offset=5,
            state=ProjectState(readme, None, None),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_asynchronous_requests_received() -> None:
    runa = Runa(User)
    kisaka_san = Pet("Kisaka-san", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
        EntityRequestReceived(
            offset=3,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Kisaka-san"},
        ),
        CreateEntityRequestSent(
            offset=4,
            trace_offset=3,
            entity_type=Pet,
            args=("Kisaka-san",),
            kwargs={"owner": runa.entity},
        ),
        CreateEntityResponseReceived(
            offset=5,
            request_offset=4,
            entity=kisaka_san,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=6,
            request_offset=3,
            response=None,
        ),
        StateChanged(
            offset=7,
            state=UserState("Yuriy", [kisaka_san]),
        ),
    ]
    assert runa.context == input_messages + output_messages


def test_cleanup_remove_processed_messages() -> None:
    runa = Runa(User)
    kisaka_san = Pet("Kisaka-san", owner=runa.entity)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
        EntityRequestReceived(
            offset=3,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Kisaka-san"},
        ),
        CreateEntityRequestSent(
            offset=4,
            trace_offset=3,
            entity_type=Pet,
            args=("Kisaka-san",),
            kwargs={"owner": runa.entity},
        ),
        CreateEntityResponseReceived(
            offset=5,
            request_offset=4,
            entity=kisaka_san,
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == [
        EntityResponseSent(
            offset=6,
            request_offset=3,
            response=None,
        ),
        StateChanged(
            offset=7,
            state=UserState("Yuriy", [kisaka_san]),
        ),
    ]
    assert runa.cleanup() == [
        EntityRequestReceived(
            offset=3,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Kisaka-san"},
        ),
        CreateEntityRequestSent(
            offset=4,
            trace_offset=3,
            entity_type=Pet,
            args=("Kisaka-san",),
            kwargs={"owner": runa.entity},
        ),
        CreateEntityResponseReceived(
            offset=5,
            request_offset=4,
            entity=kisaka_san,
        ),
        EntityResponseSent(
            offset=6,
            request_offset=3,
            response=None,
        ),
    ]
    assert runa.context == [
        StateChanged(
            offset=0,
            state=UserState("Yuriy", []),
        ),
        EntityRequestReceived(
            offset=1,
            method_name="add_pet",
            args=(),
            kwargs={"name": "Stitch"},
        ),
        CreateEntityRequestSent(
            offset=2,
            trace_offset=1,
            entity_type=Pet,
            args=("Stitch",),
            kwargs={"owner": runa.entity},
        ),
        StateChanged(
            offset=7,
            state=UserState("Yuriy", [kisaka_san]),
        ),
    ]


def test_cleanup_collapse_state_changed() -> None:
    runa = Runa(User)
    input_messages: list[ContextMessage] = [
        StateChanged(
            offset=0,
            state=UserState("Yura", []),
        ),
        StateChanged(
            offset=1,
            state=UserState("Yuriy", []),
        ),
    ]

    output_messages = runa.execute(input_messages)

    assert output_messages == []
    assert runa.cleanup() == [
        StateChanged(
            offset=0,
            state=UserState("Yura", []),
        ),
    ]
    assert runa.context == [
        StateChanged(
            offset=1,
            state=UserState("Yuriy", []),
        ),
    ]
