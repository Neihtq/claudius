# Config Notes

## Resend inbound in local dev

Resend inbound email delivery is webhook-based. For local development, that means Claudius cannot receive inbound email from Resend unless your local controller is exposed on a public HTTPS URL.

Typical setup:

```bash
uv run claudius serve --callback-url http://host.docker.internal:8000
cloudflared tunnel --url http://localhost:8000
```

Then point the Resend `email.received` webhook at the public tunnel URL plus the configured webhook path, for example:

```text
https://example.trycloudflare.com/webhook/resend
```

Notes:

- `--callback-url` is for worker containers posting replies back to the controller. It does not make the controller reachable from Resend.
- Without a tunnel or another public endpoint, local dev can still test outbound email sending, but not real inbound email delivery from Resend.
- If you want inbox-style local testing without a public webhook, the alternative would be a separate dev-only polling integration against Resend's receiving API.
