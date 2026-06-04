mock_provider "docker" {}

run "default_docker_contract" {
  command = plan

  assert {
    condition     = output.image_reference == "nginx:1.27-alpine"
    error_message = "The default lesson should use the pinned Nginx image reference."
  }

  assert {
    condition     = output.stack_names.container == "terraform-docker-workshop-dev-web"
    error_message = "The default container name should be normalized from project, environment, and service."
  }

  assert {
    condition     = output.stack_names.network == "terraform-docker-workshop-dev-network"
    error_message = "The default network name should use the shared normalized prefix."
  }

  assert {
    condition     = output.published_urls == ["http://127.0.0.1:8080"]
    error_message = "The default stack should publish the web container on localhost port 8080."
  }

  assert {
    condition     = output.common_labels["study.lesson"] == "03-docker-stack"
    error_message = "The common labels should identify the Docker stack lesson."
  }

  assert {
    condition     = output.container_settings.env == ["WORKSHOP=terraform-docker-stack"]
    error_message = "The default environment variable contract should be stable."
  }
}

run "custom_docker_contract" {
  command = plan

  variables {
    project_name = "Docker Practice"
    environment  = "sandbox"
    owner        = "Learner"
    service_name = "site"

    image_repository = "nginx"
    image_tag        = "1.27-alpine"

    published_ports = [
      {
        internal = 80
        external = 8081
        protocol = "tcp"
        ip       = "127.0.0.1"
      }
    ]

    environment_variables = {
      MODE     = "practice"
      WORKSHOP = "terraform-docker-stack"
    }

    extra_labels = {
      chapter    = "docker"
      curriculum = "terraform-basics"
    }
  }

  assert {
    condition     = output.stack_names.container == "docker-practice-sandbox-site"
    error_message = "The custom container name should follow the normalized naming contract."
  }

  assert {
    condition     = output.published_urls == ["http://127.0.0.1:8081"]
    error_message = "The custom port mapping should drive the published URL."
  }

  assert {
    condition     = output.common_labels["study.owner"] == "learner"
    error_message = "The owner label should be normalized."
  }

  assert {
    condition     = output.container_settings.env == ["MODE=practice", "WORKSHOP=terraform-docker-stack"]
    error_message = "Environment variable output should be sorted by key for stable plan review."
  }
}
