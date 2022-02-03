#!/usr/bin/env bash

pipenv install -d
pipenv run pre-commit install
npm i -g gitmoji-cli
gitmoji -i
