# Operational MVP execution matrix

Base: PR #4 merge `de657d63e4662e69dfb7169bc67aa65e8a9bda71`.
Owner prompt: `/home/dev/projects/my-data-hub/MY_DATA_HUB_ONE_PASS_COMPLETION_PROMPT.md`.

| ID | Requirement | Primary lane | Dependencies | Done when |
|---|---|---|---|---|
| R01 | Close, review and merge PR #4 | root | none | merged exact reviewed head |
| R02 | Donor baseline and single Kaggle adapter | L01 | R01 | pinned blobs and compatibility gates |
| R03 | Durable SQLite control ledger, state machine, FakeKaggle | L02 | R02 contracts | 10k property sequences and restart recovery |
| R04 | Generic runtime SDK and deterministic notebooks | L02 | R03 event contract | generated notebook/runtime contracts green |
| R05 | Real Kaggle adapter and protected/managed policy | L01 | R02,R03 | private dataset/notebook real canaries and receipts |
| R06 | PostgreSQL master, tunnel, fencing, checkpoints/recovery | L03 | R03-R05 | 3 boots, 2 rotations, verified current/previous |
| R07 | Dynamic MCP resolver, OAuth, reader/operator profiles | L04 | R03,R06 | remote HTTPS/OAuth and guarded write lifecycle |
| R08 | Read-only YDB blogger import and connector accounting | L05 | R03,R06 | full source accounting, replay no-op, checkpoint, MCP reads |
| R09 | E5/BGE-M3 separate spaces and RRF | L06 | R08 | full coverage or exact terminal exceptions, checkpoint |
| R10 | Devstand control-only deploy, DNS/TLS/autostart/reboot | root | R03,R05,R07 | merged commit deployed and reboot/port evidence |
| R11 | CI/nightly/provider workflows and sanitized receipts | root | all implementation lanes | hosted and real-provider gates green |
| R12 | XHigh security/data/split-brain audit and implementation PR merge | reviewer/root | R02-R11 | no Critical/High, PR merged |
| R13 | At least 15 real Kaggle run IDs and fault/soak matrix | L01 | R03-R09 | scenario ledger and receipts |
| R14 | Operational docs, runbooks and final receipt | root | R02-R13 | evidence-backed docs and literal final verdict |
