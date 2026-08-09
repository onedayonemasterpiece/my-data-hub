# MCP donor extraction plan

Curated source paths:

```text
private_events_mcp/access_policy.py
private_events_mcp/auth_store.py
private_events_mcp/config.py
private_events_mcp/crypto.py
private_events_mcp/limits.py
private_events_mcp/oauth.py
private_events_mcp/protocol.py
private_events_mcp/server.py
private_events_mcp/tool_catalog.py
```

Required preserved behaviour:

- supported protocol negotiation;
- OAuth resource/audience/client/scope validation;
- separate ChatGPT and code-agent policies;
- strict tool visibility and argument validation;
- bounded response cache/size/timeouts;
- correlation/audit and retry-safety errors;
- no-store/security headers and host/origin checks;
- exact action approval for destructive external side effects.

Do not import `readonly_sqlite.py` as a target storage layer and do not import
social/event tools unrelated to `my-data-hub`.
