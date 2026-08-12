# Epoch-bound master tunnel broker

## Boundary

The devstand SSH daemon accepts a reverse PostgreSQL forward only through the
dedicated `mdh-master-tunnel` account. The account has `/usr/sbin/nologin`, no
password, no authorized keys, no PTY/session channel, and no local forwarding.
Its `Match User` block permits only:

```text
AllowTcpForwarding remote
PermitListen 127.0.0.1:25432
GatewayPorts no
MaxSessions 0
```

The port is configurable at installation but remains one exact non-privileged
port. The installer does not add or alter an SSH listener, public port, VPN, edge
route, PostgreSQL service, or PGDATA path. `PermitListen` is an sshd restriction,
not an invented certificate critical option. Certificates contain only the
standard `permit-port-forwarding` extension after `clear`; sshd supplies the exact
listen restriction. This follows the upstream
[`sshd_config(5)`](https://man.openbsd.org/sshd_config) and
[`ssh-keygen(1)`](https://man.openbsd.org/ssh-keygen) contracts.

Embedding workers authenticate through a second dedicated
`mdh-embedding-worker` account. Its separate principal file and `Match User`
block permit only the opposite direction:

```text
AllowTcpForwarding local
PermitOpen 127.0.0.1:25432
PermitListen none
GatewayPorts no
MaxSessions 0
```

This does not create a public PostgreSQL listener. Deployment separately supplies
the reviewed SSH gateway host/port and hashed `known_hosts` asset; the broker only
authorizes connection from that SSH session to the existing loopback reverse-forward.

## Authenticated certificate protocol

The runtime-to-control protocol is:

1. The Notebook generates a fresh Ed25519 key under its ephemeral working runtime,
   mode `0600`. The private key is never sent to control, placed in a Dataset,
   included in Notebook output, or reused by another attempt.
2. It sends `POST /internal/runtime/tunnel-certificates/{run_id}/{attempt_id}` with
   the per-attempt runtime bearer and a body containing exactly
   `master_instance_id`, `epoch`, `public_key`, and `valid_before`.
3. The control route accepts only a one-line Ed25519 public key after verifying the
   bearer, exact run/attempt identity, REGISTERING or ACTIVE state, and exact master
   instance/epoch. ACTIVE certificate expiry may not exceed the current lease;
   REGISTERING expiry is bounded to five minutes.
4. The control container uses the mounted Unix socket and `TunnelBrokerClient` to
   call the root-owned broker. Peer UID, socket mode, exact JSON fields, and a
   32-KiB metadata ceiling are enforced before `issue_public_key(...)`. The reply
   contains only the public OpenSSH certificate, serial, epoch principal, expiry,
   and exact loopback listener. The private CA remains host-side and mode `0600`.
5. The Notebook writes the public certificate beside its ephemeral private key,
   uses both for `ssh -N -R 127.0.0.1:25432:127.0.0.1:<postgres-port>`, and deletes
   both on fence/terminal cleanup.

The control ledger must not receive the public key or certificate body. The broker
state stores only UUID/epoch/lease, serial, key ID, public-key SHA-256, expiry, and
revocation metadata. It cannot represent database URLs, passwords, rows, artifacts,
or checkpoint bytes.

## Task-bound embedding-worker certificates

Control generates each worker Ed25519 keypair outside the broker and sends only the
one-line public key. The bounded IPC action is:

```text
issue_worker_public_key(master_instance_id, epoch, task_run_id, credential_id,
                        public_key, valid_before, now)
```

The broker requires the exact ACTIVE master instance/epoch and an expiry within its
unexpired lease. It persists task/credential identity, serial, key/public-certificate
metadata and returns only public material: certificate, account, principal, expiry,
task/credential IDs, and `connect_host=127.0.0.1`, `connect_port=25432`. It cannot
accept a private key. An exact replay returns the same certificate; a changed key,
expiry, epoch or revoked identity fails closed. Cleanup calls
`revoke_worker_certificate(...)` with the exact task, credential and serial.

Epoch rotation/deactivation/expiry revokes all associated master and worker serials,
blanks both principal files and terminates sshd children for both dedicated accounts.

## Lifecycle calls

The authenticated control lifecycle maps to these broker calls:

| Control transition | Required broker call |
| --- | --- |
| new master epoch allocated for registration | `activate(instance, run, attempt, epoch, lease_until, listen_port, now)` |
| ACTIVE lease renewed | `renew(instance, run, attempt, epoch, lease_until, now)` |
| certificate requested | `issue_public_key(instance, run, attempt, epoch, public_key, valid_before, now)` |
| one certificate rejected | `revoke(instance, run, attempt, epoch, serial, reason)` |
| embedding worker admitted | `issue_worker_public_key(instance, epoch, task_run, credential, public_key, valid_before, now)` |
| embedding worker terminal | `revoke_worker_certificate(instance, epoch, task_run, credential, serial, reason)` |
| fence, terminal, rotation, or drain completion | `deactivate(instance, run, attempt, epoch, reason)` |
| every five seconds and at boot | `reconcile(now)` |

The principal, certificate key ID, and broker record bind the exact run/attempt as
well as the instance/epoch. A new identity must strictly advance the durable epoch
high-water mark. A response-loss retry for the still-active exact identity may keep or
monotonically extend its lease, but cannot shorten it, change its listener, or revive it
after expiry/deactivation. A rotation revokes every older serial and terminates only
sshd children owned by the dedicated tunnel account. Deactivation first blanks
`authorized_principals`, adds the epoch's
serials to the OpenSSH KRL, persists `active=null`, and then terminates those sessions.
The systemd timer performs the same denial when the lease expires. Missing, malformed,
oversized, or unreadable state blanks principals, revokes the CA in the KRL, terminates
the dedicated sessions, and returns failure; it never falls back to a static key.

OpenSSH checks certificate validity, the current epoch principal, the KRL, and exact
`PermitListen` on every new authentication. The timer closes already-authenticated
connections after expiry or lost lifecycle authority.

## Installation (not executed by repository tests)

Review the exact account, paths, port, and host OpenSSH include first, then explicitly run:

```bash
sudo deploy/control-plane/install_master_tunnel_broker.sh \
  INSTALL_MY_DATA_HUB_MASTER_TUNNEL_BROKER
```

The root-gated installer generates a tunnel-only Ed25519 user CA, initializes empty
authorization/KRL state, validates the sshd configuration before reload, and enables
both the fail-closed reconciliation timer and root-owned local broker service. The
ordinary control-plane installer refuses to start until the broker socket exists,
and mounts only its socket directory into the control container. Repository tests execute only syntax and
temporary-directory broker operations; they do not create an account, mutate sshd,
reload a service, or touch a real host.

The production code now activates the broker before the Kaggle run, renews it from
registration and ACTIVE heartbeats, issues a task-local key/certificate pair under
the Notebook temporary filesystem, and revokes the epoch on terminal recovery. This
is still implementation evidence only: the root installer and a real master
data-plane connection have not been executed on Devstand and must not be reported as
live acceptance.
