"""Infrastructure configuration, checked with the real tools rather than by reading it.

None of this provisions anything — `terraform apply` creates billable resources and is a
deliberate human action. What these tests establish is that the configuration is valid, that
the safety properties hold (no open SSH, actions behind the passcode, no plaintext secrets),
and that the deploy workflow is well-formed.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
TERRAFORM = REPO / "infra" / "terraform"
CADDYFILE = REPO / "infra" / "caddy" / "Caddyfile"


def tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        pytest.skip(f"{name} not installed")
    return path


def run(
    *args: str, cwd: Path | None = None, env: dict | None = None
) -> subprocess.CompletedProcess:
    return subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, env={**os.environ, **(env or {})}
    )


# ------------------------------------------------------------------ terraform


@pytest.fixture(scope="module")
def terraform_initialised() -> Path:
    tool("terraform")
    result = run("terraform", f"-chdir={TERRAFORM}", "init", "-backend=false", "-input=false")
    assert result.returncode == 0, result.stderr[-2000:]
    return TERRAFORM


def test_terraform_is_valid(terraform_initialised):
    result = run("terraform", f"-chdir={terraform_initialised}", "validate", "-json")

    report = json.loads(result.stdout)
    assert report["valid"], report.get("diagnostics")


def test_terraform_is_formatted(terraform_initialised):
    result = run("terraform", f"-chdir={terraform_initialised}", "fmt", "-check", "-recursive")

    assert result.returncode == 0, f"unformatted files:\n{result.stdout}"


def test_state_is_remote_so_infrastructure_stays_destroyable(terraform_initialised):
    """Local state is how infrastructure becomes un-destroyable when a laptop is reimaged, and
    a resource nobody can destroy keeps billing (SPEC.md §13)."""
    config = (terraform_initialised / "versions.tf").read_text()

    assert 'backend "s3"' in config


def test_ssh_is_not_open_to_the_world(terraform_initialised):
    """The firewall rule that matters. 0.0.0.0/0 on port 22 is the default mistake, so there is
    no default at all: admin_ipv4_cidrs has no `default` and Terraform will not plan without it.
    """
    variables = (terraform_initialised / "variables.tf").read_text()
    main = (terraform_initialised / "main.tf").read_text()

    ssh_rule = main.split('port       = "22"')[1].split("}")[0]
    assert "var.admin_ipv4_cidrs" in ssh_rule
    assert "0.0.0.0/0" not in ssh_rule

    admin_block = variables.split('variable "admin_ipv4_cidrs"')[1].split("\nvariable")[0]
    # No `default = ...` assignment: matching the bare word would hit the description, which
    # says "deliberately NOT defaulted".
    assert not re.search(r"^\s*default\s*=", admin_block, re.MULTILINE)


def test_only_http_and_https_are_public(terraform_initialised):
    main = (terraform_initialised / "main.tf").read_text()

    public_ports = {
        block.split('port       = "')[1].split('"')[0]
        for block in main.split("rule {")[1:]
        if "0.0.0.0/0" in block
    }
    assert public_ports == {"80", "443"}


def test_the_data_volume_is_separate_from_the_server(terraform_initialised):
    """The server is disposable; the warehouse, the source database and the golden archives
    are not. They live on an attached volume so the box can be rebuilt."""
    main = (terraform_initialised / "main.tf").read_text()

    assert 'resource "hcloud_volume" "data"' in main
    assert 'resource "hcloud_volume_attachment" "data"' in main
    assert "/data/docker" in (terraform_initialised / "cloud-init.yaml").read_text()


def test_docker_waits_for_the_data_volume():
    """Docker starting before /data is mounted silently creates a fresh data-root on the system
    disk, and every volume appears to have vanished."""
    cloud_init = (TERRAFORM / "cloud-init.yaml").read_text()

    assert "RequiresMountsFor=/data" in cloud_init


def test_the_cost_of_a_change_is_visible(terraform_initialised):
    """A sizing change should show up as a number, not as a surprise on the invoice."""
    outputs = (terraform_initialised / "outputs.tf").read_text()

    assert "monthly_cost_estimate_eur" in outputs


# ------------------------------------------------------------------ caddy


@pytest.fixture
def caddy_env() -> dict:
    return {
        "DOMAIN": "platform.example.com",
        "ACME_EMAIL": "ops@example.com",
        "DEMO_PASSCODE_HASH": "$2a$14$abcdefghijklmnopqrstuv",
    }


def test_caddyfile_is_valid(caddy_env):
    tool("caddy")
    result = run(
        "caddy", "validate", "--config", str(CADDYFILE), "--adapter", "caddyfile", env=caddy_env
    )

    assert result.returncode == 0, result.stderr[-2000:]


def test_actions_require_the_passcode_and_status_does_not(caddy_env):
    """The access model (SPEC.md §11): looking is public, changing is not."""
    config = CADDYFILE.read_text()

    actions = config.split("handle /api/actions*")[1].split("}")[0]
    status = config.split("handle /api/status*")[1].split("}")[0]

    assert "import demo_passcode" in actions
    assert "import demo_passcode" not in status


def test_airflow_ui_is_not_left_open(caddy_env):
    """Airflow has no anonymous read-only mode worth trusting, so the whole UI is gated."""
    config = CADDYFILE.read_text()

    airflow = config.split("handle /airflow*")[1].split("\t}")[0]
    assert "import demo_passcode" in airflow


def test_the_passcode_is_a_hash_from_the_environment():
    """A plaintext passcode in the repo is a passcode that stays valid after it is rotated."""
    config = CADDYFILE.read_text()

    assert "{$DEMO_PASSCODE_HASH}" in config
    assert "basic_auth" in config


def test_caddys_admin_api_is_disabled():
    """It is unauthenticated by default and reachable from any container on the same network."""
    assert "admin off" in CADDYFILE.read_text()


# ------------------------------------------------------------------ secrets


def test_no_plaintext_secrets_are_committed():
    result = run("bash", str(REPO / "scripts" / "check-secrets.sh"), cwd=REPO)

    assert result.returncode == 0, result.stdout + result.stderr


def test_sops_encrypts_values_but_leaves_keys_readable(tmp_path):
    """So a reviewer can see that a secret was added or rotated without being able to read it."""
    tool("sops")
    tool("age-keygen")

    key_file = tmp_path / "key.txt"
    keygen = run("age-keygen", "-o", str(key_file))
    public_key = next(
        line.split(": ")[1].strip()
        for line in (keygen.stderr + keygen.stdout).splitlines()
        if "public key" in line.lower()
    )

    rules = tmp_path / ".sops.yaml"
    rules.write_text(
        yaml.safe_dump(
            {
                "creation_rules": [
                    {
                        "path_regex": r".*\.yaml$",
                        "encrypted_regex": "^(.*_token|.*_password)$",
                        "age": public_key,
                    }
                ]
            }
        )
    )
    secret = tmp_path / "prod.yaml"
    secret.write_text("hcloud_token: super-secret\nserver_type: cx42\n")

    encrypted = run("sops", "--config", str(rules), "-e", "-i", str(secret), cwd=tmp_path)
    assert encrypted.returncode == 0, encrypted.stderr

    ciphertext = secret.read_text()
    assert "super-secret" not in ciphertext  # the value is protected
    assert "hcloud_token" in ciphertext  # the key is still reviewable
    assert "cx42" in ciphertext  # non-secrets stay in the clear

    decrypted = run(
        "sops",
        "--config",
        str(rules),
        "-d",
        str(secret),
        env={"SOPS_AGE_KEY_FILE": str(key_file)},
    )
    assert "super-secret" in decrypted.stdout


def test_the_secret_template_lists_every_secret_the_stack_needs():
    """A missing entry here is a deploy that fails at the last step with an empty variable."""
    template = yaml.safe_load((REPO / "secrets" / "prod.example.yaml").read_text())

    assert {
        "hcloud_token",
        "warehouse_password",
        "app_db_password",
        "airflow_api_token",
        "telegram_bot_token",
        "demo_passcode_hash",
    } <= set(template)


# ------------------------------------------------------------------ deploy pipeline

WORKFLOWS = REPO / ".github" / "workflows"


def test_workflows_are_well_formed():
    tool("actionlint")
    result = run("actionlint", *[str(p) for p in sorted(WORKFLOWS.glob("*.yml"))], cwd=REPO)

    assert result.returncode == 0, result.stdout[-3000:]


def test_images_are_tagged_with_the_commit_not_only_latest():
    """A rollback needs a name to roll back to, and "latest" is not one."""
    deploy = yaml.safe_load((WORKFLOWS / "deploy.yml").read_text())

    build = deploy["jobs"]["build"]["steps"][-1]
    assert "${{ github.sha }}" in build["with"]["tags"]


def test_only_one_deploy_runs_at_a_time_and_is_never_cancelled():
    """A half-rolled-out compose stack is worse than a queued deploy."""
    deploy = yaml.safe_load((WORKFLOWS / "deploy.yml").read_text())

    assert deploy["concurrency"]["cancel-in-progress"] is False


def test_the_rollout_verifies_health_before_reporting_success():
    """A deploy that "succeeded" while the stack is unhealthy is worse than one that failed,
    because nobody looks again."""
    script = (WORKFLOWS / "deploy.yml").read_text()

    assert "unhealthy" in script
    assert "services unhealthy after rollout" in script


def test_ddl_is_applied_on_every_deploy():
    """Every statement is CREATE ... IF NOT EXISTS, so this is a step nobody has to remember."""
    deploy = yaml.safe_load((WORKFLOWS / "deploy.yml").read_text())

    steps = [step.get("name", "") for step in deploy["jobs"]["deploy"]["steps"]]
    assert "Apply warehouse DDL" in steps


def test_secrets_reach_the_deploy_only_through_sops():
    """No secret is passed as a workflow input or committed in the compose file; the age key in
    GitHub is the single thing that unlocks the rest."""
    deploy = (WORKFLOWS / "deploy.yml").read_text()

    assert "SOPS_AGE_KEY" in deploy
    assert "sops -d secrets/prod.yaml" in deploy
