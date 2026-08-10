# Same-host MCP edge change plan

Status: `PREPARED / VPN RESTART AND OAUTH NOT YET APPROVED`

The public resource remains:

```text
https://mcp-datahub.kenigevents.ru/mcp
```

This is the requested subpath on the current permanent server. A path-only nginx edit is
not sufficient because Xray REALITY, not nginx HTTPS, currently owns TCP 443.

## Preconditions

1. `mcp-datahub.kenigevents.ru A 188.227.84.107` resolves publicly.
2. A separate trusted certificate exists under the nginx-mounted
   `secrets/tls/mcp-datahub/` directory.
3. The MCP backend passes its loopback OAuth, revocation and read-only catalog tests.
4. Current Xray and nginx configurations are copied to timestamped rollback files.
5. At least one existing VPN client is available for a regression probe.

## Controlled topology change

1. Change only the Xray REALITY listener from `0.0.0.0:443` to
   `127.0.0.1:10443` and validate with `xray run -test`.
2. Merge [`nginx-mcp-edge.conf.example`](../../deploy/same-host/nginx-mcp-edge.conf.example)
   with the existing nginx file and validate it in a disposable/current image.
3. Restart only Xray to release 443, then recreate/reload only nginx.
4. Verify the existing VPN path, the public certificate, RFC 9728 metadata, OAuth
   negatives/revocation and one bounded MCP read.

The nginx route proxies only exact `/mcp` and
`/.well-known/oauth-protected-resource/mcp`; every other path on the TLS virtual host is
404. PostgreSQL, API and port 8765 stay loopback-only. Forwarding headers are cleared so
the application does not trust spoofed client addresses.

## Rollback

Restore the saved nginx and Xray files, reload nginx so it releases 443, restart only
Xray on 443, then repeat the VPN client probe. DNS may remain during a short retry window;
delete the MCP A record if the rollback is final.

This change intentionally remains unexecuted until the controlled VPN restart and the
production OAuth issuer/JWKS are available.
