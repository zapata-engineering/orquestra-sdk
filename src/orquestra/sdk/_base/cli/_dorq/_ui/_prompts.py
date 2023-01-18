################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t

import inquirer  # type: ignore

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

    @t.overload
    def choice(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[str] = None,
    ) -> ChoiceID:
        ...

    @t.overload
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
        question = inquirer.List(
            SINGLE_INPUT,
            message=message,
            choices=choices,
            default=default,
            carousel=True,
        )
        answers = inquirer.prompt([question])

        # Workaround bad typing inside inquirer.
        assert answers is not None

        return answers[SINGLE_INPUT]

    def confirm(self, message: str, default: bool) -> bool:
        return inquirer.confirm(message, default=default)

    def checkbox(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[t.Union[str, t.List[str]]] = None,
    ) -> t.List[ChoiceID]:
        question = inquirer.Checkbox(
            SINGLE_INPUT,
            message=message
            + " (select: \u2192 | deselect: \u2190 | navigate: \u2191, \u2193)",
            choices=choices,
            default=default,
            carousel=True,
        )
        answers = inquirer.prompt([question])

        # Workaround bad typing inside inquirer.
        assert answers is not None

        return answers[SINGLE_INPUT]

    def ask_for_int(self, message: str, default: t.Optional[int]):
        def validate(_, current):
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
        return int(answers[SINGLE_INPUT])
