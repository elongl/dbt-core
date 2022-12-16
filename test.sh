#!/bin/bash

MATRIX_JSON="["
          for B in $(seq 1 5); do
              MATRIX_JSON+=$(sed 's/^/"/;s/$/"/' <<< "${B}")
          done
          MATRIX_JSON="${MATRIX_JSON//\"\"/\", \"}"
          MATRIX_JSON+="]"
          echo "split-groups=${MATRIX_JSON}"


INCLUDE_PYTHON_VERSION=("3.8")
INCLUDE_OS=("windows-latest" "macos-latest")
INCLUDE_GROUPS="["
          for group in $(seq 1 5); do
            for python_version in ${INCLUDE_PYTHON_VERSION[@]}; do
                for os in ${INCLUDE_OS[@]}; do
                    INCLUDE_GROUPS+=$(sed 's/$/, /' <<< "{\"split-group\": \"${group}\", \"python-version\": \"${python_version}\", \"os\": \"${os}\"}")
                done
            done
          done
          INCLUDE_GROUPS=$(echo $INCLUDE_GROUPS | sed 's/,*$//g')
          INCLUDE_GROUPS+="]"
          echo "include=${INCLUDE_GROUPS}"