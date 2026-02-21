# Execution completion

Execution completion is a way of executing domain model logic, inspired by chat completion.

Let's imagine, that the domain model cannot cause side effects, but it can pause execution with the intention of requesting another entity or external service.

For example, the domain model consists of classes and synchronous methods:
```python
from dataclasses import dataclass

from execution_completion.model import Entity


@dataclass
class UserState:
    name: str


class User(Entity):
    def __init__(self, name: str) -> None:
        self.name = name

    def __getstate__(self) -> UserState:
        return UserState(self.name)

    def __setstate__(self, state: UserState) -> None:
        self.name = state.name

    def write_article(self, text: str) -> Article:
        return Article(self, text)

    def write_comment(self, article: Article, text: str) -> Comment:
        comment = Comment(self, text)
        article.add_comment(comment)
        return comment


@dataclass
class ArticleState:
    author: User
    text: str
    comments: list[Comment]


class Article(Entity):
    def __init__(self, author: User, text: str) -> None:
        self.author = author
        self.text = text
        self.comments: list[Comment] = []

    def __getstate__(self) -> ArticleState:
        return ArticleState(self.author, self.text, self.comments)

    def __setstate__(self, state: ArticleState) -> None:
        self.author = state.author
        self.text = state.text
        self.comments = state.comments

    def add_comment(self, comment: Comment) -> None:
        self.comments.append(comment)


@dataclass
class CommentState:
    author: User
    text: str


class Comment(Entity):
    def __init__(self, author: User, text: str) -> None:
        self.author = author
        self.text = text

    def __getstate__(self) -> CommentState:
        return CommentState(self.author, self.text)

    def __setstate__(self, state: CommentState) -> None:
        self.author = state.author
        self.text = state.text
```

The `Execution.complete` method accepts input messages and produces output messages, even if the execution is suspended.

```python
def test_entity_method_request_sent() -> None:
    yura = User("Yura")
    article = yura.write_article("Execution completion")

    execution = Execution(User)
    comment = Comment(execution.subject, "Bullshit")

    input_messages: list[ContextMessage] = [
        EntityStateChanged(
            offset=31337,
            state=UserState("Guru"),
        ),
        EntityMethodRequestReceived(
            offset=31338,
            method=User.write_comment,
            args=(article,),
            kwargs={"text": "Bullshit"},
        ),
        CreateEntityRequestSent(
            offset=31339,
            trace_offset=31338,
            entity_type=Comment,
            args=(execution.subject, "Bullshit"),
            kwargs={},
        ),
        CreateEntityResponseReceived(
            offset=31340,
            request_offset=31339,
            response=comment,
        ),
    ]

    output_messages = execution.complete(input_messages)

    assert output_messages == [
        EntityMethodRequestSent(
            offset=31341,
            trace_offset=31338,
            receiver=article,
            method=Article.add_comment,
            args=(comment,),
            kwargs={},
        )
    ]
```
