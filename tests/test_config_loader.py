import pytest
from pathlib import Path
from claudius.config.loader import ConfigError, load_startup_config, load_workflows, resolve_startup_config

FIXTURES = Path(__file__).parent / "fixtures" / "workflows"

def test_load_valid_workflow():
    workflows = load_workflows(FIXTURES / "valid.yaml")
    assert len(workflows) == 1
    assert workflows[0].name == "test-workflow"
    assert workflows[0].routing.channels == ["email"]

def test_load_directory(tmp_path):
    import shutil
    shutil.copy(FIXTURES / "valid.yaml", tmp_path / "valid.yaml")
    workflows = load_workflows(tmp_path)
    names = [w.name for w in workflows]
    assert "test-workflow" in names

def test_load_invalid_raises():
    with pytest.raises(ConfigError):
        load_workflows(FIXTURES / "invalid.yaml")

def test_load_nonexistent_raises():
    with pytest.raises(FileNotFoundError):
        load_workflows(Path("/nonexistent/path"))


def test_load_example_workflow():
    workflows = load_workflows(Path("config/workflows/example.yaml"))
    assert len(workflows) == 1
    tool_names = [tool.name for tool in workflows[0].runtime.tools]
    assert "merge_mr_and_close_session" in tool_names


def test_load_startup_config_returns_none_when_missing(tmp_path):
    assert load_startup_config(tmp_path / "claudius.yaml") is None


def test_load_shipped_startup_config():
    config = load_startup_config(Path("config/claudius.yaml"))
    assert config is not None
    assert config.upstream_llm.protocol == "anthropic"
    assert config.upstream_llm.auth_mode == "bearer"


def test_load_invalid_startup_config_raises(tmp_path):
    path = tmp_path / "claudius.yaml"
    path.write_text("upstream_llm:\n  protocol: nope\n")
    with pytest.raises(ConfigError):
        load_startup_config(path)


def test_resolve_startup_config_applies_protocol_defaults(tmp_path):
    path = tmp_path / "claudius.yaml"
    path.write_text("upstream_llm:\n  protocol: openai\n")
    config = resolve_startup_config(load_startup_config(path))
    assert config.upstream_llm.protocol == "openai"
    assert config.upstream_llm.base_url == "https://api.openai.com/v1"
    assert config.upstream_llm.api_key_env == "OPENAI_API_KEY"
    assert config.upstream_llm.auth_mode == "bearer"


def test_resolve_startup_config_cli_overrides_file(tmp_path):
    path = tmp_path / "claudius.yaml"
    path.write_text(
        "image: custom:from-file\n"
        "upstream_llm:\n"
        "  protocol: anthropic\n"
        "  api_key_env: FILE_KEY\n"
        "  auth_mode: x-api-key\n"
    )
    config = resolve_startup_config(
        load_startup_config(path),
        {
            "image": "custom:from-cli",
            "proxy_upstream_kind": "openai",
            "proxy_upstream_api_key_env": "CLI_KEY",
        },
    )
    assert config.image == "custom:from-cli"
    assert config.upstream_llm.protocol == "openai"
    assert config.upstream_llm.base_url == "https://api.openai.com/v1"
    assert config.upstream_llm.api_key_env == "CLI_KEY"
    assert config.upstream_llm.auth_mode == "x-api-key"  # file value preserved when not overridden
