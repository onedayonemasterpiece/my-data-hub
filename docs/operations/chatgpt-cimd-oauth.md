# ChatGPT CIMD public-client OAuth

The provider-only MCP profile supports ChatGPT's preferred Client ID Metadata Document
(CIMD) path in addition to the existing predefined static clients. It does not implement
dynamic client registration and does not create or accept a client secret.

When enabled, OAuth discovery publishes
`client_id_metadata_document_supported: true`, public token authentication method `none`,
and PKCE method `S256`. ChatGPT sends a stable MCP-specific
`https://chatgpt.com/.../client.json` URL as `client_id`. The authorization server:

- accepts only the exact `https://chatgpt.com` origin, with no port, credentials, query,
  fragment, or dot path segments;
- fetches with a three-second timeout, a 32-KiB response cap, JSON content type, HTTP 200,
  and no redirects;
- requires the document `client_id` to be an exact string match;
- requires one exact `https://chatgpt.com/connector/oauth/{callback_id}` redirect;
- accepts only authorization-code/refresh public-client metadata with method `none`, then
  still enforces S256 PKCE and the exact MCP `resource` on authorization and token calls;
- rejects shared-secret metadata, inline keys, malformed/oversized documents, and any
  unapproved scope;
- caches only the validated nonsecret client ID, redirect, and server-owned scope policy,
  for at most five minutes and 16 client IDs. Errors are never cached.

Provider-only deployment enables CIMD with exactly `openid`, `offline_access`,
`platform:read`, `provider:read`, and `provider:write`. The existing static client path
remains ledger-gated and compatible. No live OAuth or deployment mutation was performed
as part of this implementation.
