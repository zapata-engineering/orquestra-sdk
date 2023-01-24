################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t
import warnings
from typing import overload

from orquestra.sdk import exceptions

# One of our transitive dependencies shows DeprecationWarnings related to invalid usage
# of distutils. There's nothing we can do about it, so until it's fixed upstream we can
# safely ignore it. See: https://github.com/fmoo/python-editor/issues/35
warnings.filterwarnings("ignore", module="editor")

import inquirer  # type: ignore # noqa

SINGLE_INPUT = "single_input"


ChoiceID = str
T = t.TypeVar("T")


class Prompter:
    """
    This is the last layer before handing off the interaction to ``inquirer``, a
    3rd-party library.

    Given it's our system's boundary it would be nice to write tests that perform real
    stdin/stdout IO, but simulating arrow key strokes is very tricky! ATM this class
    isn't covered by tests.
    """

    @overload
    def choice(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[str] = None,
    ) -> ChoiceID:
        ...

    @overload
    def choice(
        self,
        choices: t.Sequence[t.Tuple[ChoiceID, T]],
        message: str,
        default: t.Optional[str] = None,
    ) -> T:
        ...

    def choice(
        self,
        choices: t.Sequence[t.Union[ChoiceID, t.Tuple[ChoiceID, T]]],
        message: str,
        default: t.Optional[str] = None,
    ) -> t.Union[ChoiceID, T]:
        """
        Presents the user a choice and returns what they selected

        Args:
            choices: The list of choices to present to the user. If this is of the shape
                ``(label, value)`` then the ``label`` is shown to the user, but
                ``value`` is what is returned.
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.

        Returns:
            The item the user chose, either a ChoiceID or an object if ``choices`` was
            a tuple.

        Raises:
            UserCancelledPrompt if the user cancels the prompt
        """

        question = inquirer.List(
            SINGLE_INPUT,
            message=message,
            choices=choices,
            default=default,
            carousel=True,
        )
        answers = inquirer.prompt([question])

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answers is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        return answers[SINGLE_INPUT]

    def confirm(self, message: str, default: bool) -> bool:
        """
        Ask the user for confirmation

        Args:
            message: The message to prompt the user.
            default: The value to return as the default.

        Returns:
            The result from the prompt

        Raises:
            UserCancelledPrompt if the user cancels the prompt
        """
        answer = inquirer.confirm(message, default=default)

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answer is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        # Fixing typing issues from inquirer
        assert isinstance(answer, bool)

        return answer

    @overload
    def checkbox(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> t.List[ChoiceID]:
        ...

    @overload
    def checkbox(
        self,
        choices: t.Sequence[t.Tuple[ChoiceID, T]],
        message: str,
        default: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> t.List[T]:
        ...

    def checkbox(
        self,
        choices: t.Sequence[t.Union[ChoiceID, t.Tuple[ChoiceID, T]]],
        message: str,
        default: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> t.Union[t.List[ChoiceID], t.List[T]]:
        """
        Presents the user a multiple choice and returns what they selected

        Args:
            choices: The list of choices to present to the user. If this is of the shape
                ``(label, value)`` then the ``label`` is shown to the user, but
                ``value`` is what is returned.
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.

        Returns:
            The list of items the user chose, either ChoiceIDs or objects if
            ``choices`` was a tuple.

        Raises:
            UserCancelledPrompt if the user cancels the prompt
        """

        question = inquirer.Checkbox(
            SINGLE_INPUT,
            message=message
            + " (select: \u2192 | deselect: \u2190 | navigate: \u2191, \u2193)",
            choices=choices,
            default=default,
            carousel=True,
        )
        answers = inquirer.prompt([question])

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answers is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        return answers[SINGLE_INPUT]

    def ask_for_int(
        self,
        message: str,
        default: t.Optional[int],
        allow_none: t.Optional[bool] = False,
    ) -> t.Union[int, None]:
        """
        Asks the user to enter an integer

        If the user's input is not an integer, the prompt will ask again

        Args:
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.
            allow_none: allow 'None' as a valid response.

        Returns:
            an integer parsed from the user's input

        Raises:
            UserCancelledPrompt if the user cancels the prompt
        """

        nonestrings: t.List[str] = []
        if allow_none:
            nonestrings = ["", "None"]

        def validate(_, current):
            if current in nonestrings:
                return True

            try:
                int(current)
            except ValueError as e:
                raise inquirer.errors.ValidationError(
                    "", reason="Value must be an integer."
                ) from e

            return True

        question = inquirer.Text(
            name=SINGLE_INPUT, message=message, default=default, validate=validate
        )

        answers = inquirer.prompt([question])

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answers is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        # If the user enters one of the strings representing `None`, return None.
        if answers[SINGLE_INPUT] in nonestrings:
            return None

        return int(answers[SINGLE_INPUT])

    def ask_for_str(
        self,
        message: str,
        default: t.Optional[str],
        allow_none: t.Optional[bool] = True,
    ) -> t.Union[str, None]:
        """
        Asks the user to enter a string.

        Args:
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.

        Returns:
            an integer parsed from the user's input

        Raises:
            UserCancelledPrompt if the user cancels the prompt
        """

        nonestrings: t.List[str] = []
        if allow_none:
            nonestrings = ["", "None"]

        question = inquirer.Text(name=SINGLE_INPUT, message=message, default=default)

        answers = inquirer.prompt([question])

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answers is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        # If the user enters one of the strings representing `None`, return None.
        if answers[SINGLE_INPUT] in nonestrings:
            return None

        return answers[SINGLE_INPUT]
