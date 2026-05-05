import json
import os

import httpx
from click.testing import CliRunner
import pytest
from unittest.mock import MagicMock, patch

from claudius.cli import _validate_workflow_models_against_upstream_pricing, cli
from claudius.config.schema import WorkflowConfig

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "runtime-mcp-bridge" in result.output
    assert "runtime-sidecar" in result.output
    assert "serve" in result.output
    assert "session" in result.output
    assert "session-history" in result.output

def test_serve_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--help"])
    assert result.exit_code == 0
    assert "openai" in result.output
    assert "litellm" not in result.output
    assert "cerebras" not in result.output

def test_session_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["session", "--help"])
    assert result.exit_code == 0


def test_session_history_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["session-history", "--help"])
    assert result.exit_code == 0


def test_session_history_fetches_and_prints_json():
    runner = CliRunner()
    response = MagicMock()
    response.json.return_value = [{"role": "user", "content": "hello"}]
    response.raise_for_status.return_value = None
    env = {
        "CLAUDIUS_CALLBACK_URL": "http://controller",
        "CLAUDIUS_SESSION_ID": "sess-1",
        "CLAUDIUS_SESSION_TOKEN": "token-1",
    }

    with patch("claudius.cli.httpx.get", return_value=response) as mock_get:
        result = runner.invoke(
            cli,
            ["session-history", "--side", "user", "--limit", "5", "--query", "hello"],
            env=env,
        )

    assert result.exit_code == 0
    mock_get.assert_called_once_with(
        "http://controller/sessions/sess-1/history",
        params={"side": "user", "limit": 5, "query": "hello"},
        headers={"Authorization": "Bearer token-1"},
        timeout=10.0,
    )
    assert json.loads(result.output) == [{"role": "user", "content": "hello"}]


def test_session_history_requires_env():
    runner = CliRunner()
    result = runner.invoke(cli, ["session-history"], env={})
    assert result.exit_code != 0
    assert "CLAUDIUS_CALLBACK_URL is required" in result.output


def test_session_history_surfaces_http_errors():
    runner = CliRunner()
    request = httpx.Request("GET", "http://controller/sessions/sess-1/history")
    response = httpx.Response(500, request=request, text="boom")
    env = {
        "CLAUDIUS_CALLBACK_URL": "http://controller",
        "CLAUDIUS_SESSION_ID": "sess-1",
        "CLAUDIUS_SESSION_TOKEN": "token-1",
    }

    with patch("claudius.cli.httpx.get", return_value=response):
        result = runner.invoke(cli, ["session-history"], env=env)

    assert result.exit_code != 0
    assert "history fetch failed: boom" in result.output


def test_validate_workflow_models_against_upstream_pricing_rejects_unknown_models():
    workflows = [
        WorkflowConfig.model_validate({
            "name": "test-workflow",
            "routing": {"channels": ["email"]},
            "claude": {"model": "unknown-model", "system_prompt": "x"},
        })
    ]
    with pytest.raises(Exception, match="Unknown workflow model"):
        _validate_workflow_models_against_upstream_pricing(
            workflows,
            {
                "known-model": {
                    "input_cost_per_million_tokens_usd": 0.6,
                    "output_cost_per_million_tokens_usd": 1.2,
                }
            },
        )
