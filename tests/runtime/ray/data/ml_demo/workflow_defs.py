################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
from typing import Tuple

import numpy as np
from sklearn.linear_model import LinearRegression  # type: ignore

import orquestra.sdk as sdk


@sdk.task
def generate_data(size: int, a: float, b: float, *args):
    x = np.arange(size)
    y = a * x + 5 * np.random.random(size) + b
    return (x.reshape(-1, 1), y.reshape(-1, 1))


@sdk.task
def train_model1(data: Tuple) -> LinearRegression:
    x = data[0]
    y = data[1]
    model = LinearRegression()
    model.fit(x, y)
    return model


@sdk.workflow
def orquestra_basic_demo():
    size = 20
    a = 1
    b = 4
    # x, y = generate_data(size, a, b)
    data = generate_data(size, a, b, "first")
    # x, y = sdk.split_outputs(generate_data(size, a, b), 2)
    model = train_model1(data)
    return [data, model]
