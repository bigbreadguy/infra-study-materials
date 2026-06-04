# Lesson 03: Docker Stack

## Goal

Practice Terraform with a real local provider lifecycle. This lesson uses the
Docker provider to manage a small local web stack: image, network, volume, and
container. The focus is provider initialization, resource replacement behavior,
explicit cleanup, and reading a graph that now talks to an external API.

Confidence: 94/100.

## What This Builds

- A pinned Docker image reference for a local Nginx container.
- A dedicated Docker bridge network for the lesson.
- A named Docker volume attached to the container.
- A container with labels, environment variables, memory limits, and published
  localhost ports.
- A Terraform contract object that summarizes the stack in plan output.

Confidence: 92/100.

## Files

- `main.tf`: provider requirements, Docker resources, labels, and lifecycle
  teaching notes.
- `variables.tf`: typed inputs and validation for image tags, ports, labels, and
  Docker connection settings.
- `outputs.tf`: values to inspect after `plan` or `apply`.
- `tests/lesson_contract.tftest.hcl`: plan-time checks using a mocked Docker
  provider.

Confidence: 93/100.

## Prerequisites

- Terraform CLI installed.
- Docker Desktop or a compatible Docker Engine running.
- Permission for Terraform to talk to the Docker API socket.

On macOS with Docker Desktop, the Docker CLI may use a socket similar to:

```sh
unix:///Users/kenny/.docker/run/docker.sock
```

If the default provider connection cannot find Docker, pass the socket
explicitly:

```sh
terraform plan -var='docker_host=unix:///Users/kenny/.docker/run/docker.sock'
```

Confidence: 88/100.

## Commands

Run from this directory:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
terraform test
```

`terraform test` uses a mocked Docker provider, so it verifies the Terraform
contract without creating containers. `terraform plan` and `terraform apply`
talk to the real Docker API.

Confidence: 93/100.

## Optional Local Apply

Only run this when Docker is running locally and you are comfortable creating
local Docker objects:

```sh
terraform apply
```

After apply, inspect the local endpoint:

```sh
curl http://127.0.0.1:8080
docker ps --filter 'name=terraform-docker-workshop-dev-web'
docker network inspect terraform-docker-workshop-dev-network
docker volume inspect terraform-docker-workshop-dev-html
```

Clean up when finished:

```sh
terraform destroy
```

Confidence: 91/100.

## Things To Observe

1. `docker_image.web` uses a pinned image tag instead of `latest`, so changes to
   the image are reviewable.
   Confidence: 92/100.

2. `docker_container.web` depends implicitly on the image, network, and volume
   because it references their attributes.
   Confidence: 95/100.

3. Changing `image_tag`, `service_name`, published ports, or the uploaded index
   content can force container recreation. This is the first lesson where
   replacement affects a real running object.
   Confidence: 90/100.

4. The host port is bound to `127.0.0.1` by default. That keeps the workshop
   reachable from your machine without exposing it on every network interface.
   Confidence: 92/100.

5. Labels mirror cloud tagging practices. The syntax is Docker-specific, but the
   habit maps directly to GCP labels later.
   Confidence: 94/100.

## Workshop Exercise

1. Run `terraform plan` with the defaults and identify the image, network,
   volume, and container resources.
2. Change the external port:

   ```sh
   terraform plan -var='published_ports=[{internal=80,external=8081,protocol="tcp",ip="127.0.0.1"}]'
   ```

3. Change `image_tag` to another pinned Nginx tag and compare replacement
   behavior.
4. Add an environment variable and inspect how the planned container changes:

   ```sh
   terraform plan -var='environment_variables={WORKSHOP="terraform-docker-stack",MODE="practice"}'
   ```

5. After an optional apply, stop the container manually with Docker and run
   `terraform plan` again. Notice how Terraform detects drift.

Confidence: 90/100.

## Production Note

This is still a learning stack. In production, container scheduling would usually
belong to a platform such as Kubernetes, Cloud Run, or a managed orchestration
service rather than standalone Docker on a laptop. The Terraform lessons still
transfer: pin versions, review plans, label resources, keep state safe, and
understand what changes force replacement.

Confidence: 93/100.
