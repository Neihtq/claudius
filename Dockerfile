FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client nodejs npm curl ca-certificates \
    && GLAB_VERSION=$(curl -s https://gitlab.com/api/v4/projects/gitlab-org%2Fcli/releases | python3 -c "import sys,json; data=json.load(sys.stdin); print(next(r['tag_name'].lstrip('v') for r in data if not r.get('upcoming_release', False)))") \
    && ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/') \
    && curl -sL "https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_${ARCH}.deb" -o /tmp/glab.deb \
    && dpkg -i /tmp/glab.deb \
    && rm /tmp/glab.deb \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv
RUN npm install -g @anthropic-ai/claude-code @playwright/mcp playwright
RUN playwright install --with-deps chromium

COPY pyproject.toml uv.lock .
COPY src/ src/

RUN UV_LINK_MODE=copy uv sync --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# Build the web UI so the controller can serve it at /ui (server.py mounts
# /app/ui/dist when present).
COPY ui/ ui/
RUN npm --prefix ui ci && npm --prefix ui run build

COPY docker/entrypoint.sh /usr/local/bin/claudius-entrypoint
RUN chmod +x /usr/local/bin/claudius-entrypoint

ENTRYPOINT ["/usr/local/bin/claudius-entrypoint"]
CMD ["serve"]
