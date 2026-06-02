run "default_module_contract" {
  command = plan

  assert {
    condition     = output.name_prefix == "terraform-module-workshop-dev"
    error_message = "The default name prefix should be normalized by the label_set module."
  }

  assert {
    condition     = output.common_labels.environment == "dev"
    error_message = "The composed labels should include the default environment."
  }

  assert {
    condition     = keys(output.track_summaries) == ["core", "practice"]
    error_message = "The default lesson should compose the core and practice learning path modules."
  }

  assert {
    condition     = output.track_summaries.core.topic_count == 3
    error_message = "The core track should expose three normalized topics."
  }

  assert {
    condition     = output.required_tracks == ["core"]
    error_message = "Only the core track should be required by default."
  }

  assert {
    condition     = output.checkpoint_matrix.practice == ["practice-checkpoint-1", "practice-checkpoint-2"]
    error_message = "The practice track should expose two generated checkpoints."
  }
}

run "custom_module_contract" {
  command = plan

  variables {
    workspace_name = "Reusable Workshop"
    environment    = "sandbox"
    owner          = "Learner"

    extra_labels = {
      chapter    = "modules"
      curriculum = "terraform"
    }

    learning_tracks = {
      foundations = {
        description      = "Custom module contract test."
        topics           = ["inputs", "outputs"]
        checkpoint_count = 1
        required         = true
      }
    }
  }

  assert {
    condition     = output.name_prefix == "reusable-workshop-sandbox"
    error_message = "The custom workspace and environment should drive the normalized prefix."
  }

  assert {
    condition     = output.common_labels.owner == "learner"
    error_message = "The label module should normalize owner values."
  }

  assert {
    condition     = keys(output.track_summaries) == ["foundations"]
    error_message = "The custom run should create only the foundations module instance."
  }

  assert {
    condition     = output.track_summaries.foundations.topic_count == 2
    error_message = "The custom foundations track should expose two topics."
  }

  assert {
    condition     = output.checkpoint_matrix.foundations == ["foundations-checkpoint-1"]
    error_message = "The custom foundations track should expose one checkpoint."
  }
}
