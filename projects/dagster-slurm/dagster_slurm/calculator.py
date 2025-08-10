"""
A very simple calculator module.

This module provides basic arithmetic operations. It's designed to demonstrate
how documentation can be generated from docstrings.
"""


class Calculator:
    """A class that performs calculations.

    This is a simple example of a class that can be documented.

    Attributes:
        last_result (float): The result of the last calculation.
    """

    def __init__(self):
        """Initializes the Calculator."""
        self.last_result = 0.0

    def add(self, a: float, b: float) -> float:
        """Adds two numbers together.

        Args:
            a (float): The first number.
            b (float): The second number.

        Returns:
            float: The sum of the two numbers.

        Raises:
            TypeError: If inputs are not numeric.
        """
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            raise TypeError("Both inputs must be numeric")
        result = a + b
        self.last_result = result
        return result


def subtract(a: float, b: float) -> float:
    """A standalone function to subtract two numbers.

    Args:
        a (float): The number to subtract from.
        b (float): The number to subtract.

    Returns:
        float: The difference between a and b.
    """
    return a - b
