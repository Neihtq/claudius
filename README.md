# Claudius

A message-driven agentic platform. Receives inbound messages (email via Resend, WhatsApp via Meta), routes them to persistent sessions keyed by thread ID, and runs Claude-powered workflows in isolated Docker containers.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker (for running session workers)
- Node.js 18+ (only needed to modify the UI; pre-built assets are included)
- An upstream LLM API key. The shipped `config/claudius.yaml` defaults to OpenRouter, so local dev usually needs an `OPENROUTER_API_KEY`.

## Local dev setup

```bash
# Install Python dependencies
uv sync

# Build the Docker image used to run session workers
docker build -t claudius:latest .

# Review and adjust startup defaults if needed.
# The default file enables OpenRouter and includes commented examples
# for Anthropic, OpenAI, Cerebras, and LiteLLM.
$EDITOR config/claudius.yaml

# Set the API key for the active upstream config in config/claudius.yaml
export OPENROUTER_API_KEY=sk-or-...

# Start the controller with the local dev UI enabled
# Use host.docker.internal so worker containers can reach back to this process
uv run claudius serve \
  --callback-url http://host.docker.internal:8000 \
  --workspaces-path /tmp/claudius-workspaces
```

> Rebuild the image (`docker build -t claudius:latest .`) whenever you change code that runs inside session workers (`src/claudius/session/`, `src/claudius/channels/`, `src/claudius/config/`).

Then open **http://localhost:8000/ui/** in your browser.

The `--callback-url` flag enables the local dev channel: session workers will POST their replies back to the controller instead of sending real emails, and replies appear in the browser via Server-Sent Events.

## Using the UI

1. Click **+ New Session** and fill in the channel, workflow, and message.
2. Click **Start Session** — this injects a synthetic inbound message and launches a worker container.
3. The conversation view opens automatically. Agent replies stream in as they arrive.
4. Follow-up replies reuse the same workspace and start a fresh worker execution against the existing repository state.

## Configuring workflows

Workflow definitions live in `config/workflows/`. Each file is a YAML document:

```yaml
name: gitlab-assistant
routing:
  channels: [email]
  from:
    - "*@example.com"
  subject_patterns:
    - "GitLab:*"
claude:
  model: claude-opus-4-7
  system_prompt: |
    You are a software engineering agent working inside /workspace.
    The repository is prepared under /workspace/repo by runtime hooks.
    Use the available runtime tools for authenticated GitLab operations.
    Use your built-in tools for local Git operations such as diffing,
    committing, and running checks.
    If the user later confirms they are happy with the result, call
    merge_mr_and_close_session to merge the MR and end the session.
runtime:
  pre_launch:
    - type: shell
      when: session_start
      run: |
        : "${GITLAB_TOKEN:?GITLAB_TOKEN is required for GitLab runtime hooks}"
        export GIT_TERMINAL_PROMPT=0
        if [ ! -d "$REPO_DIR/.git" ]; then
          AUTH_HEADER="$(printf 'oauth2:%s' "$GITLAB_TOKEN" | base64 | tr -d '\n')"
          git -c credential.helper= \
            -c "http.extraHeader=Authorization: Basic $AUTH_HEADER" \
            clone "https://${REPO_HOST}/${PROJECT_PATH}.git" "$REPO_DIR"
        fi
        cd "$REPO_DIR"
        git config user.name "Claudius"
        git config user.email "claudius@example.com"
    - type: shell
      when: execution_start
      run: |
        : "${GITLAB_TOKEN:?GITLAB_TOKEN is required for GitLab runtime hooks}"
        export GIT_TERMINAL_PROMPT=0
        cd "$REPO_DIR"
        AUTH_HEADER="$(printf 'oauth2:%s' "$GITLAB_TOKEN" | base64 | tr -d '\n')"
        git -c credential.helper= \
          -c "http.extraHeader=Authorization: Basic $AUTH_HEADER" \
          fetch origin
  tool_context:
    - name: GITLAB_TOKEN
      env: GITLAB_TOKEN
    - name: REPO_HOST
      value: gitlab.com
    - name: PROJECT_PATH
      value: your-group/your-repo
    - name: REPO_DIR
      value: /workspace/repo
    - name: BRANCH
      value: claudius/${{request.thread_id}}
  tools:
    - type: shell
      name: git_push
      description: Push the runtime-managed branch only if changes are limited to src/ and README.md
      run: |
        : "${GITLAB_TOKEN:?GITLAB_TOKEN is required for GitLab runtime tools}"
        export GIT_TERMINAL_PROMPT=0
        cd "$REPO_DIR"
        AUTH_HEADER="$(printf 'oauth2:%s' "$GITLAB_TOKEN" | base64 | tr -d '\n')"
        git -c credential.helper= \
          -c "http.extraHeader=Authorization: Basic $AUTH_HEADER" \
          fetch origin develop
        changed_files="$(git diff --name-only origin/develop...HEAD)"
        test -n "$changed_files"
        invalid_files="$(printf '%s\n' "$changed_files" | grep -vE '^(src/|README\.md$)' || true)"
        test -z "$invalid_files"
        git -c credential.helper= \
          -c "http.extraHeader=Authorization: Basic $AUTH_HEADER" \
          push origin "$BRANCH:$BRANCH"
    - type: shell
      name: create_mr
      description: Create a merge request from the runtime-managed branch
      run: cd "$REPO_DIR" && glab mr create --source-branch "$BRANCH" --target-branch develop --title "$INPUT_TITLE" --description "$INPUT_BODY"
      params:
        title: { type: string, description: Merge request title }
        body: { type: string, description: Merge request description }
    - type: shell
      name: merge_mr_and_close_session
      description: Squash merge the MR for the runtime-managed branch, delete the source branch, and close the current Claudius session
      run: |
        cd "$REPO_DIR"
        export GLAB_TOKEN="$GITLAB_TOKEN"
        glab mr merge "$BRANCH" --squash --delete-source-branch
        curl --fail --silent --show-error \
          -X POST \
          -H "Authorization: Bearer $CLAUDIUS_SESSION_TOKEN" \
          "${CLAUDIUS_CALLBACK_URL%/}/sessions/${CLAUDIUS_SESSION_ID}/close"
response:
  channel: email
session:
  timeout_minutes: 60
  max_messages: 50
```

Global controller startup settings now live in `config/claudius.yaml`. The controller reads this file on startup if present, then applies any explicit `claudius serve` CLI flags as overrides.

The controller still loads all workflow `*.yaml` files from the configured workflow directory on startup. By default that is `config/workflows/`, and you can override it in `config/claudius.yaml` or with `--config-dir`.

Runtime tools are exposed to Claude through an MCP bridge. Tool params are passed to commands as `INPUT_*` environment variables, so a param like `title` becomes `$INPUT_TITLE`. Secret-bearing values from `runtime.tool_context` stay in the isolated runtime sidecar and are not injected into the Claude worker container.

When the controller callback channel is enabled, runtime tools also receive `CLAUDIUS_CALLBACK_URL`, `CLAUDIUS_SESSION_ID`, and `CLAUDIUS_SESSION_TOKEN` so a trusted workflow tool can call back into controller-owned endpoints such as session close.

For `runtime.tool_context`, use `env` for environment variables and `vault_read` for Vault/OpenBao reads. `vault_read` accepts `path` and an optional `field`:

```yaml
tool_context:
  - name: GITLAB_TOKEN
    env: GITLAB_TOKEN
  - name: API_TOKEN
    vault_read:
      path: secret/data/my-app
      field: token
```

## UI development

To get hot-module replacement while working on the frontend, run the Vite dev server alongside the backend. The Vite dev server proxies all API and SSE requests to the backend automatically.

```bash
# Terminal 1 — backend
ANTHROPIC_API_KEY=sk-ant-... \
uv run claudius serve --callback-url http://host.docker.internal:8000 --workspaces-path /tmp/claudius-workspaces

# Terminal 2 — frontend
cd ui && npm run dev
```

Then open **http://localhost:5173/ui/** instead of the backend port. The `base: '/ui/'` config in `vite.config.ts` keeps the paths consistent between dev and production.

## Proxy upstream modes

The controller proxy now selects an upstream adapter by protocol, not by vendor name:

- `anthropic` — Anthropic Messages-compatible upstreams
- `openai` — OpenAI-style bearer-auth upstreams
- `litellm` — LiteLLM’s Anthropic-compatible proxy mode with request rewriting

The shipped `config/claudius.yaml` uses the `anthropic` protocol with OpenRouter’s base URL and bearer auth. Use that file for persistent defaults, and use `--proxy-upstream-kind`, `--proxy-upstream-url`, and `--proxy-upstream-api-key-env` when you want a one-off override.

### Using OpenRouter directly

This is the simplest route if you want Anthropic-format requests to reach OpenRouter without LiteLLM in the middle:

```bash
export OPENROUTER_API_KEY=sk-or-...

uv run claudius serve \
  --callback-url http://host.docker.internal:8000 \
  --workspaces-path /tmp/claudius-workspaces \
  --proxy-upstream-kind anthropic
```

Example OpenRouter config:

- protocol: `anthropic`
- base URL: `https://openrouter.ai/api`
- auth mode: `bearer`
- auth env var: `OPENROUTER_API_KEY`

OpenRouter exposes an Anthropic-compatible Messages API at `/api/v1/messages`, so the worker can keep using the Anthropic SDK format directly.

### Using OpenRouter via LiteLLM

If you still want LiteLLM in the middle, start the included proxy:

```bash
OPENROUTER_API_KEY=sk-or-... docker compose up -d
```

Then map your model names in `config/litellm.yaml`. The `model_name` must match the model specified in each workflow YAML. The `model:` value is the OpenRouter model ID (see [openrouter.ai/models](https://openrouter.ai/models)):

```yaml
model_list:
  - model_name: claude-haiku-4-5-20251001   # name used in workflow YAML
    litellm_params:
      model: openrouter/anthropic/claude-3-5-haiku
      api_key: os.environ/OPENROUTER_API_KEY
```

Start the controller in `litellm` mode:

```bash
uv run claudius serve \
  --callback-url http://host.docker.internal:8000 \
  --workspaces-path /tmp/claudius-workspaces \
  --proxy-upstream-kind litellm
```

Defaults for `litellm` mode:

- base URL: `http://localhost:4000`
- auth mode: `x-api-key`
- auth env var: none

Worker containers never talk to the upstream directly. They authenticate to the controller proxy via a short-lived JWT, and the controller proxy handles the upstream protocol details.

## Running tests

```bash
uv run pytest
```

## Production (Kubernetes)

See `k8s/` for controller deployment, RBAC, and PVC manifests. Set `--image` to your built image and configure a real `RESEND_API_KEY`.

The `claudius session <id>` command is the worker entry point; the controller spawns it as a Pod via the Kubernetes API.
