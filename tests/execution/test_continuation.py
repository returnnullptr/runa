from abc import abstractmethod
from dataclasses import dataclass
from textwrap import dedent

from runa import Execution
from runa.context import (
    ContextMessage,
    CreateEntityRequestReceived,
    EntityMethodRequestReceived,
    EntityStateChanged,
    ServiceMethodRequestSent,
    ServiceMethodResponseReceived,
)
from runa.model import Entity, Service


class LLM(Service):
    @abstractmethod
    def complete(self, prompt: str) -> str: ...


@dataclass
class ProjectState:
    description: str
    tests: str
    code: str


class Project(Entity):
    llm: LLM

    def __init__(self, description: str) -> None:
        self.description = dedent(description)
        self.tests = self.llm.complete(
            prompt=dedent("""
                Project: {description}
                Follow TDD red phase to write a test.
                The `test_project.py`:
            """).format(
                description=self.description,
            )
        )
        self.code = self.llm.complete(
            prompt=dedent("""
                Project: {description}
                The `test_project.py`:
                {tests}
                Follow TDD green phase to write code.
                The `project.py`:
            """).format(
                description=self.description,
                tests=self.tests,
            )
        )

    def __getstate__(self) -> ProjectState:
        return ProjectState(self.description, self.tests, self.code)

    def __setstate__(self, state: ProjectState) -> None:
        self.description = state.description
        self.tests = state.tests
        self.code = state.code

    def fix_tests(self, error: str) -> None:
        self.tests = self.llm.complete(
            prompt=dedent("""
                Project: {description}
                The `project.py`:
                {code}
                The `test_project.py`:
                {tests}
                Fix this error in tests: {error}
                Updated `test_project.py`:
            """).format(
                description=self.description,
                tests=self.tests,
                code=self.code,
                error=error,
            )
        )

    def fix_code(self, error: str) -> None:
        self.code = self.llm.complete(
            prompt=dedent("""
                Project: {description}
                The `project.py`:
                {code}
                The `test_project.py`:
                {tests}
                Fix this error in code: {error}
                Updated `project.py`:
            """).format(
                description=self.description,
                tests=dedent(self.tests),
                code=dedent(self.code),
                error=error,
            )
        )


def test_continuous_execution() -> None:
    execution = Execution(Project)
    input_messages: list[ContextMessage] = [
        CreateEntityRequestReceived(
            offset=0,
            args=("Calculator",),
            kwargs={},
        ),
    ]

    assert execution.complete(input_messages) == [
        ServiceMethodRequestSent(
            offset=1,
            trace_offset=0,
            service_type=LLM,
            method_name="complete",
            args=(),
            kwargs={
                "prompt": dedent("""
                    Project: Calculator
                    Follow TDD red phase to write a test.
                    The `test_project.py`:
                """)
            },
        ),
    ]

    execution.cleanup()
    input_messages = execution.context + [
        ServiceMethodResponseReceived(
            offset=2,
            request_offset=1,
            response=dedent("""
                def test_add() -> None:
                    assert add(2, 2) == 5
            """).strip(),
        )
    ]

    assert execution.complete(input_messages) == [
        ServiceMethodRequestSent(
            offset=3,
            trace_offset=0,
            service_type=LLM,
            method_name="complete",
            args=(),
            kwargs={
                "prompt": dedent("""
                    Project: Calculator
                    The `test_project.py`:
                    def test_add() -> None:
                        assert add(2, 2) == 5
                    Follow TDD green phase to write code.
                    The `project.py`:
                """)
            },
        ),
    ]

    execution.cleanup()
    input_messages = execution.context + [
        ServiceMethodResponseReceived(
            offset=4,
            request_offset=3,
            response=dedent("""
                def add(a: int, b: int) -> int:
                    return a + b + 1
            """).strip(),
        )
    ]

    execution.complete(input_messages)
    execution.cleanup()

    assert execution.context == [
        EntityStateChanged(
            offset=6,
            state=ProjectState(
                description="Calculator",
                tests=dedent("""
                    def test_add() -> None:
                        assert add(2, 2) == 5
                """).strip(),
                code=dedent("""
                    def add(a: int, b: int) -> int:
                        return a + b + 1
                """).strip(),
            ),
        )
    ]

    input_messages = execution.context + [
        EntityMethodRequestReceived(
            offset=7,
            method_name="fix_tests",
            args=("2 + 2 is 4",),
            kwargs={},
        )
    ]

    assert execution.complete(input_messages) == [
        ServiceMethodRequestSent(
            offset=8,
            trace_offset=7,
            service_type=LLM,
            method_name="complete",
            args=(),
            kwargs={
                "prompt": dedent("""
                    Project: Calculator
                    The `project.py`:
                    def add(a: int, b: int) -> int:
                        return a + b + 1
                    The `test_project.py`:
                    def test_add() -> None:
                        assert add(2, 2) == 5
                    Fix this error in tests: 2 + 2 is 4
                    Updated `test_project.py`:
                """)
            },
        )
    ]

    execution.cleanup()
    input_messages = execution.context + [
        EntityMethodRequestReceived(
            offset=9,
            method_name="fix_code",
            args=("No need to add 1 to the result",),
            kwargs={},
        )
    ]

    assert execution.complete(input_messages) == [
        ServiceMethodRequestSent(
            offset=10,
            trace_offset=9,
            service_type=LLM,
            method_name="complete",
            args=(),
            kwargs={
                "prompt": dedent("""
                    Project: Calculator
                    The `project.py`:
                    def add(a: int, b: int) -> int:
                        return a + b + 1
                    The `test_project.py`:
                    def test_add() -> None:
                        assert add(2, 2) == 5
                    Fix this error in code: No need to add 1 to the result
                    Updated `project.py`:
                """)
            },
        )
    ]

    execution.cleanup()
    input_messages = execution.context + [
        ServiceMethodResponseReceived(
            offset=11,
            request_offset=10,
            response=dedent("""
                def add(a: int, b: int) -> int:
                    return a + b
            """).strip(),
        )
    ]

    execution.complete(input_messages)

    execution.cleanup()
    assert execution.context == [
        EntityStateChanged(
            offset=6,
            state=ProjectState(
                description="Calculator",
                tests=dedent("""
                    def test_add() -> None:
                        assert add(2, 2) == 5
                """).strip(),
                code=dedent("""
                    def add(a: int, b: int) -> int:
                        return a + b + 1
                """).strip(),
            ),
        ),
        EntityMethodRequestReceived(
            offset=7,
            method_name="fix_tests",
            args=("2 + 2 is 4",),
            kwargs={},
        ),
        ServiceMethodRequestSent(
            offset=8,
            trace_offset=7,
            service_type=LLM,
            method_name="complete",
            args=(),
            kwargs={
                "prompt": dedent("""
                    Project: Calculator
                    The `project.py`:
                    def add(a: int, b: int) -> int:
                        return a + b + 1
                    The `test_project.py`:
                    def test_add() -> None:
                        assert add(2, 2) == 5
                    Fix this error in tests: 2 + 2 is 4
                    Updated `test_project.py`:
                """)
            },
        ),
        EntityStateChanged(
            offset=13,
            state=ProjectState(
                description="Calculator",
                tests=dedent("""
                    def test_add() -> None:
                        assert add(2, 2) == 5
                """).strip(),
                code=dedent("""
                    def add(a: int, b: int) -> int:
                        return a + b
                """).strip(),
            ),
        ),
    ]
    input_messages = execution.context + [
        ServiceMethodResponseReceived(
            offset=14,
            request_offset=8,
            response=dedent("""
                def test_add() -> None:
                    assert add(2, 2) == 4
            """).strip(),
        )
    ]

    execution.complete(input_messages)

    execution.cleanup()
    assert execution.context == [
        EntityStateChanged(
            offset=16,
            state=ProjectState(
                description="Calculator",
                tests=dedent("""
                    def test_add() -> None:
                        assert add(2, 2) == 4
                """).strip(),
                code=dedent("""
                    def add(a: int, b: int) -> int:
                        return a + b
                """).strip(),
            ),
        ),
    ]
