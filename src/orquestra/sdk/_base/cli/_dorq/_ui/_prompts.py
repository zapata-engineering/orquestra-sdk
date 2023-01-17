################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################

import typing as t

import inquirer  # type: ignore

SINGLE_INPUT = "single_input"


ChoiceID = str


class Prompter:
    """
    This is the last layer before handing off the interaction to ``inquirer``, a
    3rd-party library.

    Given it's our system's boundary it would be nice to write tests that perform real
    stdin/stdout IO, but simulating arrow key strokes is very tricky! ATM this class
    isn't covered by tests.
    """

    def choice(
        self,
        choices: t.Sequence[ChoiceID],
        message: str,
        default: t.Optional[str] = None,
    ) -> ChoiceID:
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
