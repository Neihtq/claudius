import fnmatch
from claudius.config.schema import WorkflowConfig
from claudius.models import InboundMessage


def match_workflow(
    message: InboundMessage, workflows: list[WorkflowConfig]
) -> WorkflowConfig | None:
    for workflow in workflows:
        r = workflow.routing
        if message.channel not in r.channels:
            continue
        if r.from_ and not any(
            fnmatch.fnmatch(message.sender, pat) for pat in r.from_
        ):
            continue
        if r.to and not any(
            fnmatch.fnmatch(recipient, pat)
            for recipient in message.recipients
            for pat in r.to
        ):
            continue
        if r.subject_patterns:
            subj = message.subject or ""
            if not any(fnmatch.fnmatch(subj, pat) for pat in r.subject_patterns):
                continue
        return workflow
    return None
