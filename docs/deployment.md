# Deployment

How the live instance is provisioned, deployed to, and — the part most demos skip — taken down.

## What it costs

| Resource | Spec | Monthly |
|---|---|---|
| Hetzner CX42 | 8 vCPU, 16 GB, 160 GB | ~€17 |
| Attached volume | 100 GB | ~€4.40 |
| Object storage | Terraform state + golden archives | ~€1 |
| **Total** | | **~€22** |

`make cost` prints the estimate for whatever is currently configured. The sizing is not
arbitrary: the full stack needs about 13 GB (SPEC.md §2), so 16 GB leaves roughly 3 GB of
headroom. CX32 cannot run it; CX52 doubles the cost for headroom nothing uses.

## Teardown discipline

The failure this prevents is a €400 invoice three weeks after the demo, for resources nobody
remembers creating.

- **Everything is created by Terraform.** Nothing is clicked into existence in the Hetzner
  console, because a resource Terraform does not know about survives `destroy` and keeps
  billing.
- **State lives in object storage**, not on a laptop. Local state is how infrastructure becomes
  un-destroyable after a reimage.
- **`make destroy` removes all of it**, volume included. There is no partial teardown that
  leaves "just the data" behind quietly accruing charges.
- **Set a budget alert in the Hetzner console** as a backstop. It is the one thing here that
  Terraform cannot express, and the one thing that catches a mistake the code did not.

## First provision

```bash
cd infra/terraform
cp backend.hcl.example backend.hcl            # bucket + endpoint
cp terraform.tfvars.example terraform.tfvars  # your SSH key and your IP, not 0.0.0.0/0

export AWS_ACCESS_KEY_ID=…                    # Hetzner object storage key pair
export AWS_SECRET_ACCESS_KEY=…
export TF_VAR_hcloud_token="$(sops -d ../../secrets/prod.yaml | yq -r .hcloud_token)"

terraform init -backend-config=backend.hcl
make plan      # read this properly: it is the last point before anything is billable
make apply
```

Then point DNS at `terraform output server_ipv4` and wait for cloud-init (`ssh deploy@… tail -f
/var/log/cloud-init-output.log`).

## Deploying

Merging to `main` builds every service image, tags it with the commit SHA, pushes to GHCR, and
rolls the stack over. See [.github/workflows/deploy.yml](../.github/workflows/deploy.yml).

Three deliberate choices in there:

- **Images are built in CI, not on the box.** Building on a 16 GB server that is also running
  the platform is how a deploy takes the platform down.
- **Tags are commit SHAs, not just `latest`.** A rollback needs a name to roll back to, and
  "latest" is not one: `IMAGE_TAG=<sha> docker compose up -d`.
- **The rollout is health-checked.** A deploy that reports success while the stack is unhealthy
  is worse than one that fails, because nobody looks again.

## Secrets

`sops` + `age`, with the encrypted file committed. That sounds backwards and is the opposite:
the alternative is secrets living only in someone's shell history and a GitHub secret, where
nobody can see what exists or when it was rotated. Here every change is a reviewable diff, and
only the values are encrypted — the keys stay readable, so a reviewer can see that a secret was
added without being able to read it.

```bash
make secrets-edit      # decrypt, edit, re-encrypt on save
make secrets-check     # fails if plaintext is about to be committed (also runs in CI)
```

Rotating someone out means removing their age public key from `.sops.yaml` and re-encrypting.

## Access model

Caddy terminates TLS and decides what the public may reach (SPEC.md §11):

| Path | Access |
|---|---|
| `/`, `/api/status*`, `/app*`, `/grafana*` | public, read-only |
| `/airflow*` | demo passcode |
| `/api/actions*` (run pipeline, chaos, reset) | demo passcode |

The passcode is stored as a bcrypt hash from the environment, so the plaintext is never in the
repo or on the box. `caddy hash-password` produces it.

## What has and has not been verified

Everything here is validated by the real tools, in CI and in
[tests/test_infra_config.py](../tests/test_infra_config.py): `terraform validate` and `fmt`,
`caddy validate`, `actionlint`, a genuine sops/age encrypt-decrypt round-trip, and assertions
on the safety properties (SSH is never open to the world, actions require the passcode, no
plaintext secrets).

**Not verified: `terraform apply`.** It creates billable resources, so it is a deliberate human
action — nobody should discover a running server because a test ran. The milestone's
"apply/destroy round-trips cleanly" is therefore still open, and stays open until someone runs
it with real credentials.
