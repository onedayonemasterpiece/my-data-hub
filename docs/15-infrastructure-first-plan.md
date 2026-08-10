# Infrastructure-first plan — corrected topology

The first infrastructure proof is not a local database deployment. It is a safe lightweight
control plane plus deterministic lifecycle tests.

Gates:

1. PR-A authority, incident, invariants and removal of local-master paths;
2. donor runtime compatibility;
3. FakeKaggle lifecycle/state-machine, lease/fencing and registry tests;
4. real private provider lifecycle smoke;
5. master Notebook restore/start/gate/checkpoint/rotation PoC;
6. dynamic MCP and synthetic connector against ACTIVE master;
7. durability and bounded canary;
8. Region Talk later.

The control plane is healthy at `master=ABSENT`. It never substitutes an embedded local
PostgreSQL, canonical business catalog or local backup. Disposable CI PostgreSQL remains
only to validate schemas, roles and migration compatibility.
