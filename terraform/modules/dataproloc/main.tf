resource "google_dataproc_workflow_template" "template" {
  name = "template-example"
  location = "us-central1-a"
  placement {
    managed_cluster {
      cluster_name = "my-cluster"
      config {
        gce_cluster_config {
          zone = "us-central1-a"
          tags = ["foo", "bar"]
        }
        master_config {
          num_instances = 1
          machine_type = "n1-standard-1"
          disk_config {
            boot_disk_type = "pd-ssd"
            boot_disk_size_gb = 15
          }
        }
        worker_config {
          num_instances = 3
          machine_type = "n1-standard-2"
          disk_config {
            boot_disk_size_gb = 10
            num_local_ssds = 2
          }
        }

        secondary_worker_config {
          num_instances = 2
        }
        software_config {
          image_version = "2.0.35-debian10"
        }
      }
    }
  }
  jobs {
    step_id = "someJob"
    spark_job {
      main_class = "SomeClass"
    }
  }
  jobs {
    step_id = "otherJob"
    prerequisite_step_ids = ["someJob"]
    presto_job {
      query_file_uri = "someuri"
    }
  }
}