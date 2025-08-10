# API Reference

This is the auto-generated API reference for the `dagster-slurm` library.
It is generated directly from the docstrings in the Python source code.

## Core

This section covers the main components of the library.

<a id="module-dagster_slurm"></a>

### dagster_slurm.hello() → str

## Calculator Example

A simple module to demonstrate functionality.

<a id="module-dagster_slurm.calculator"></a>

A very simple calculator module.

This module provides basic arithmetic operations. It’s designed to demonstrate
how documentation can be generated from docstrings.

### *class* dagster_slurm.calculator.Calculator

Bases: `object`

A class that performs calculations.

This is a simple example of a class that can be documented.

#### last_result

The result of the last calculation.

* **Type:**
  float

#### add(a: float, b: float) → float

Adds two numbers together.

* **Parameters:**
  * **a** (*float*) – The first number.
  * **b** (*float*) – The second number.
* **Returns:**
  The sum of the two numbers.
* **Return type:**
  float
* **Raises:**
  **TypeError** – If inputs are not numeric.

### dagster_slurm.calculator.subtract(a: float, b: float) → float

A standalone function to subtract two numbers.

* **Parameters:**
  * **a** (*float*) – The number to subtract from.
  * **b** (*float*) – The number to subtract.
* **Returns:**
  The difference between a and b.
* **Return type:**
  float
