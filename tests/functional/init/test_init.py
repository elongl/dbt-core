import pytest

from dbt.tests.util import run_dbt
from unittest import mock
import os


class TestInit:
    @pytest.fixture(scope="class")
    def project_name(self, unique_schema):
        return f'my_project_{unique_schema}'

    @mock.patch('dbt.task.init._get_adapter_plugin_names')
    @mock.patch('click.confirm')
    @mock.patch('click.prompt')
    def test_init_provided_project_name_and_skip_profile_setup(self, mock_prompt, mock_confirm, mock_get, project, project_name):
        manager = mock.Mock()
        manager.attach_mock(mock_prompt, 'prompt')
        manager.attach_mock(mock_confirm, 'confirm')

        # Start by removing the dbt_project.yml so that we're not in an existing project
        os.remove(os.path.join(project.project_root, 'dbt_project.yml'))

        manager.prompt.side_effect = [
            1,
            'localhost',
            5432,
            'test_username',
            'test_password',
            'test_db',
            'test_schema',
            4,
        ]
        mock_get.return_value = ['postgres']

        # provide project name through the ini command
        run_dbt(['init', project_name, '-s'])
        manager.assert_not_called()

        with open(os.path.join(project.project_root, project_name, 'dbt_project.yml'), 'r') as f:
            assert f.read() == f"""
# Name your project! Project names should contain only lowercase characters
# and underscores. A good package name should reflect your organization's
# name or the intended use of these models
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

# In this example config, we tell dbt to build all models in the example/
# directory as views. These settings can be overridden in the individual model
# files using the `{{{{ config(...) }}}}` macro.
models:
  {project_name}:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
"""
