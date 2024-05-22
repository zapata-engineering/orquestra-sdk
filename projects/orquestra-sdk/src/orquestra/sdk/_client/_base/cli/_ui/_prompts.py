################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t
import warnings
from typing import overload

from inquirer import errors  # type: ignore # noqa
from orquestra.workflow_shared import exceptions

# One of our transitive dependencies shows DeprecationWarnings related to invalid usage
# of distutils. There's nothing we can do about it, so until it's fixed upstream we can
# safely ignore it. See: https://github.com/fmoo/python-editor/issues/35
warnings.filterwarnings("ignore", module="editor")

import inquirer  # type: ignore # noqa

SINGLE_INPUT = "single_input"


ChoiceID = str
T = t.TypeVar("T")


class Prompter:
    """This is the last layer before handing off the interaction to ``inquirer``.

    ``inquirer`` is a 3rd-party library.

    Given it's our system's boundary it would be nice to write tests that perform real
    stdin/stdout IO, but simulating arrow key strokes is very tricky!
    ATM this class isn't covered by tests.
    """

    @overload
    def choice(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[str] = None,
        allow_all: t.Optional[bool] = False,
    ) -> ChoiceID:
        ...

    @overload
    def choice(
        self,
        choices: t.Sequence[t.Tuple[ChoiceID, T]],
        message: str,
        default: t.Optional[str] = None,
        allow_all: t.Optional[bool] = False,
    ) -> T:
        ...

    def choice(
        self,
        choices: t.Sequence[t.Union[ChoiceID, t.Tuple[ChoiceID, T]]],
        message: str,
        default: t.Optional[str] = None,
        allow_all: t.Optional[bool] = False,
    ) -> t.Union[ChoiceID, T]:
        """Presents the user a choice and returns what they selected.

        If only one option is available, the user is prompted to confirm that this is
        the intended outcome and, if so, that option is selected automatically.

        Args:
            choices: The list of choices to present to the user. If this is of the shape
                ``(label, value)`` then the ``label`` is shown to the user, but
                ``value`` is what is returned.
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.
            allow_all: if True, add an 'all' option to the bottom of the list.

        Returns:
            The item the user chose, either a ChoiceID or an object if ``choices`` was
                a tuple.

        Raises:
            UserCancelledPrompt: if the user cancels the prompt.
        """
        # If there are no choices, report it to the user and exit.
        if len(choices) == 0:
            raise exceptions.NoOptionsAvailableError(message)

        # If there's only one choice, select it automatically and confirm with the user
        # that that's what they want to do.
        if len(choices) == 1:
            return self._handle_single_option(message, choices[0])

        # We may need to be able to modify the list of choices to add an 'all' option,
        # so cast it to a list instance.
        _choices = list(choices)
        if allow_all:
            _choices += ["all"]

        question = inquirer.List(
            SINGLE_INPUT,
            message=message,
            choices=_choices,
            default=default,
            carousel=True,
        )
        answers = inquirer.prompt([question])

        # If the user cancels the prompt, via ctrl-c, answers will be `None`.
        if answers is None:
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")

        return answers[SINGLE_INPUT]

    def confirm(self, message: str, default: bool) -> bool:
        """Ask the user for confirmation.

        Args:
            message: The message to prompt the user.
            default: The value to return as the default.

        Returns:
            The result from the prompt.

        Raises:
            UserCancelledPrompt: if the user cancels the prompt.
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
        """Presents the user a multiple choice and returns what they selected.

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
            UserCancelledPrompt: if the user cancels the prompt.
        """
        # If there are no choices, report it to the user and exit.
        if len(choices) == 0:
            raise exceptions.NoOptionsAvailableError(message)

        # If there's only one choice, select it automatically and confirm with the user
        # that that's what they want to do.
        if len(choices) == 1:
            return t.cast(
                t.Union[t.List[ChoiceID], t.List[T]],
                [self._handle_single_option(message, choices[0])],
            )

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
        """Asks the user to enter an integer.

        If the user's input is not an integer, the prompt will ask again.

        Args:
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.
            allow_none: allow 'None' as a valid response.

        Returns:
            an integer parsed from the user's input.

        Raises:
            UserCancelledPrompt: if the user cancels the prompt.
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
                raise errors.ValidationError(
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
        """Asks the user to enter a string.

        Args:
            message: The message to prompt the user.
            default: The value to return as the default, if the user doesn't choose
                anything.
            allow_none: If True, the prompter will accept 'None' or an empty string as
                a response, and return None if one of these is given.

        Returns:
            an integer parsed from the user's input.

        Raises:
            UserCancelledPrompt: if the user cancels the prompt.
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

    def _handle_single_option(
        self, message: str, choice: t.Union[ChoiceID, t.Tuple[ChoiceID, T]]
    ) -> t.Union[ChoiceID, T]:
        """Prompt for confirmation.

        This method exists to avoid the scenario of asking a user to choose between 1
        option.
        """
        name: ChoiceID
        value: t.Union[ChoiceID, T]
        # When the choice is a tuple, we unpack the display name and the returned value.
        # Otherwise, the choice is a ChoiceID and should be used as both the display
        # name and the returned value.
        if isinstance(choice, tuple):
            name, value = choice
        else:
            name, value = choice, choice

        if not self.confirm(
            f"{message} - only one option is available. Proceed with {name}?",
            default=True,
        ):
            raise exceptions.UserCancelledPrompt(f"User cancelled {message} prompt")
        return value
