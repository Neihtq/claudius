import re
from dataclasses import dataclass
from claudius.models import InboundMessage

PATTERN = re.compile(r'\$\{\{(.+?)\}\}')


@dataclass
class TemplateContext:
    request: InboundMessage
    session_id: str
    channel_name: str


def expand(value: str, ctx: TemplateContext) -> str:
    def replace(match: re.Match) -> str:
        expr = match.group(1).strip()
        parts = expr.split(".", 1)
        if len(parts) != 2:
            return match.group(0)
        ns, key = parts
        if ns == "request":
            val = getattr(ctx.request, key, None)
            return str(val) if val is not None else match.group(0)
        if ns == "session" and key == "id":
            return ctx.session_id
        if ns == "channel" and key == "name":
            return ctx.channel_name
        return match.group(0)

    return PATTERN.sub(replace, value)


def expand_dict(obj: dict | list | str, ctx: TemplateContext) -> dict | list | str:
    """Recursively expand templates in a dict/list/str."""
    if isinstance(obj, str):
        return expand(obj, ctx)
    if isinstance(obj, dict):
        return {k: expand_dict(v, ctx) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_dict(item, ctx) for item in obj]
    return obj
