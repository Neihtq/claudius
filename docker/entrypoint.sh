#!/bin/sh
# Claudius container entrypoint.
#
# Backward compatible with `docker run <image> <subcommand> [args...]`, which
# runs `claudius <subcommand> [args...]`. In addition, if CLAUDIUS_ROLE is set,
# it selects the subcommand to run (ignoring CMD). This lets a single image serve
# multiple roles in deployments that pick the role via an environment variable
# rather than a per-container command override (e.g. Rise):
#
#   CLAUDIUS_ROLE=serve         -> claudius serve
#   CLAUDIUS_ROLE=session-pod   -> claudius session-pod
set -e

if [ -n "${CLAUDIUS_ROLE:-}" ]; then
    exec claudius "${CLAUDIUS_ROLE}"
fi

exec claudius "$@"
