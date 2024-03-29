#!/bin/bash

set -e

THIS_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
THIS_SCRIPT_DIR="$(dirname "$THIS_SCRIPT")"
PYTHON_REPO_DIR="$(dirname "$THIS_SCRIPT_DIR")"
PYTHON_SOURCE_DIR="${PYTHON_REPO_DIR}/$(basename "$PYTHON_REPO_DIR")"

(cd "$PYTHON_REPO_DIR"
	python3 -m poetry run flake8 --min-python-version 3.10.0
	python3 -m poetry run mypy "$PYTHON_SOURCE_DIR"
)
