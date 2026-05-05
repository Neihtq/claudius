import pytest

from claudius.config.schema import StartupConfig, WorkflowConfig

MINIMAL_YAML_DICT = {
    "name": "test-workflow",
    "routing": {"channels": ["email"]},
    "claude": {"system_prompt": "You are a test assistant."},
    "response": {"channel": "email"},
}

FULL_YAML_DICT = {
    "name": "gitlab-assistant",
    "description": "Handles GitLab tasks",
    "routing": {
        "channels": ["email"],
        "from": ["user@example.com", "*@company.com"],
        "subject_patterns": ["GitLab:*"],
    },
    "claude": {
        "model": "claude-opus-4-7",
        "system_prompt": "You are a GitLab assistant.",
        "tools": [{"plugin": "gitlab", "allowed_projects": ["org/repo"]}],
    },
    "runtime": {
        "pre_launch": [{"type": "shell", "when": "execution_start", "run": "git fetch"}],
        "tool_context": [{"name": "BRANCH", "value": "claudius/${{request.thread_id}}"}],
        "tools": [
            {
                "type": "shell",
                "name": "git_push",
                "description": "Push the branch",
                "run": "git push origin \"$BRANCH:$BRANCH\"",
                "params": {"title": {"type": "string"}},
            }
        ],
    },
    "response": {"channel": "email"},
    "session": {"timeout_minutes": 30, "max_messages": 20},
}

def test_minimal_workflow_config():
    wf = WorkflowConfig.model_validate(MINIMAL_YAML_DICT)
    assert wf.name == "test-workflow"
    assert wf.claude.model == "claude-opus-4-7"
    assert wf.session.timeout_minutes == 60
    assert wf.session.active_followup_policy == "interrupt_after_turn"

def test_full_workflow_config():
    wf = WorkflowConfig.model_validate(FULL_YAML_DICT)
    assert wf.routing.from_ == ["user@example.com", "*@company.com"]
    assert wf.routing.subject_patterns == ["GitLab:*"]
    assert wf.session.timeout_minutes == 30
    assert len(wf.claude.tools) == 1
    assert wf.claude.tools[0]["plugin"] == "gitlab"
    assert wf.runtime.tool_context[0].name == "BRANCH"
    assert wf.runtime.tools[0].name == "git_push"


def test_invalid_active_followup_policy_fails():
    with pytest.raises(Exception):
        WorkflowConfig.model_validate({
            **MINIMAL_YAML_DICT,
            "session": {"active_followup_policy": "later"},
        })

def test_routing_channels_required():
    with pytest.raises(Exception):
        WorkflowConfig.model_validate({
            "name": "x",
            "routing": {},
            "claude": {"system_prompt": "x"},
            "response": {"channel": "email"},
        })


def test_runtime_param_name_collision_fails():
    with pytest.raises(Exception):
        WorkflowConfig.model_validate({
            "name": "x",
            "routing": {"channels": ["email"]},
            "claude": {"system_prompt": "x"},
            "runtime": {
                "tools": [
                    {
                        "name": "tool_one",
                        "description": "x",
                        "run": "true",
                        "params": {
                            "foo-bar": {"type": "string"},
                            "foo_bar": {"type": "string"},
                        },
                    }
                ]
            },
        })


def test_runtime_vault_read_field_must_not_be_empty():
    with pytest.raises(Exception):
        WorkflowConfig.model_validate({
            "name": "x",
            "routing": {"channels": ["email"]},
            "claude": {"system_prompt": "x"},
            "runtime": {
                "tool_context": [
                    {
                        "name": "API_TOKEN",
                        "vault_read": {"path": "secret/data/my-app", "field": "   "},
                    }
                ]
            },
        })


def test_startup_config_defaults_to_anthropic_protocol_with_x_api_key_auth():
    cfg = StartupConfig.model_validate({})
    assert cfg.upstream_llm.protocol == "anthropic"
    assert cfg.upstream_llm.auth_mode is None
    assert cfg.image == "claudius:latest"


def test_startup_config_rejects_invalid_protocol():
    with pytest.raises(Exception):
        StartupConfig.model_validate({
            "upstream_llm": {"protocol": "bogus"},
        })


def test_startup_config_accepts_model_pricing():
    cfg = StartupConfig.model_validate({
        "upstream_llm": {
            "protocol": "openai",
            "model_pricing": {
                "qwen-3-235b-a22b-instruct-2507": {
                    "input_cost_per_million_tokens_usd": 0.6,
                    "output_cost_per_million_tokens_usd": 1.2,
                }
            },
        }
    })
    pricing = cfg.upstream_llm.model_pricing["qwen-3-235b-a22b-instruct-2507"]
    assert pricing.input_cost_per_million_tokens_usd == 0.6
    assert pricing.output_cost_per_million_tokens_usd == 1.2


def test_startup_config_rejects_negative_model_pricing():
    with pytest.raises(Exception):
        StartupConfig.model_validate({
            "upstream_llm": {
                "protocol": "openai",
                "model_pricing": {
                    "qwen": {
                        "input_cost_per_million_tokens_usd": -1,
                        "output_cost_per_million_tokens_usd": 1,
                    }
                },
            }
        })
