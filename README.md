# Fig Data Challenge

This project represents a standard data job configuration at Fig and can be used for interview purposes.

If you look at the `data/restaurant_data.xlsx`, there are restaurant menu items available.

## Pyenv (Optional)

We recommend using [pyenv](https://github.com/pyenv/pyenv#installation) to manage your Python installations. This will allow you to safely use Python versions different from your system's default version.

This project is currently on Python **3.11**. To install this version with pyenv:

```bash
pyenv install 3.11.1
```

## Poetry

This project uses [Poetry](python-poetry.org) to manage its packages and virtual environments. Poetry handles both installing the correct package versions, as well as ensuring those packages are available during runtime. See installation instructions [here](https://python-poetry.org/docs/#installation).

## Tooling Setup

To setup the project, run:

```bash
make setup
```

This will install any needed packages as well as setup the [pre-commit](https://pre-commit.com/) hooks. We currently use the following tools when committing code:

- [flake8](https://github.com/PyCQA/flake8) - A linter.
- [black](https://github.com/psf/black) - A code formatter. We are currently using a line length of [90 characters](https://www.youtube.com/watch?v=wf-BqAjZb8M&t=260s).
- [mypy](https://github.com/python/mypy) - A static type checker.
- [isort](https://github.com/PyCQA/isort) - A package import formatter.

If you'd like to run these tools manually, run:

```bash
poetry run pre-commit run --all-files
```

or:

```bash
make pre-commit
```
