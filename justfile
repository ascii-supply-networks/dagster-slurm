# Default recipe to display available commands
default:
    @just --list

# Format all code in root project
fmt:
    pixi run -e build --frozen fmt

# Lint all code in root project
lint:
    pixi run -e build --frozen lint

# Format code in examples project
fmt-examples:
    cd examples && pixi run -e ci-basics --frozen fmt

# Lint code in examples project (formatting only, no type checking)
lint-examples:
    cd examples && pixi run -e ci-basics --frozen ruff check ./projects && pixi run -e ci-basics --frozen dprint check

# Lint examples with full type checking (may show import errors in ci-basics env)
lint-examples-full:
    cd examples && pixi run -e ci-basics --frozen lint

# Format both root and examples
fmt-all: fmt fmt-examples

# Lint both root and examples (formatting only)
lint-all: lint lint-examples

# Run all checks (format + lint) on root
check: fmt lint

# Run all checks (format + lint) on examples
check-examples: fmt-examples lint-examples

# Run all checks on both root and examples
check-all: fmt-all lint-all

# Format documentation markdown
fmt-docs:
    pixi run -e build --frozen fmt-docs

# Check documentation markdown formatting
lint-docs:
    pixi run -e build --frozen lint-docs

# Serve documentation locally (includes API docs and slides)
docs-serve:
    pixi run -e docs --frozen docs-serve

# Build documentation for production
docs-build:
    pixi run -e docs --frozen docs-build

# Serve slides in development mode
slides:
    pixi run -e docs --frozen slides

# Build slides as static files
slides-build:
    pixi run -e docs --frozen slides-build

# Export slides to PDF
slides-export-pdf:
    pixi run -e docs --frozen slides-export-pdf
