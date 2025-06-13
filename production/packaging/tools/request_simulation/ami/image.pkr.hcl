// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

variable "regions" {
  type    = list(string)
  validation {
    condition     = length(var.regions) > 0
    error_message = <<EOF
The regions var is not set. Must specify at least one region.
EOF
  }
}

variable "commit_version" {
  type    = string
}

# Directory path where the built artifacts appear
variable "distribution_dir" {
  type = string
  default = env("DIST")
  validation {
    condition     = length(var.distribution_dir) > 0
    error_message = <<EOF
The distribution_dir var is not set: make sure to at least set the DIST env var.
To fix this you could also set the distribution_dir variable from the arguments, for example:
$ packer build -var=distribution_dir=/src/workspace/dist/aws ...
EOF
  }
}

# Directory path of the key/value project repository
variable "workspace" {
  type = string
  default = env("WORKSPACE")
  validation {
    condition     = length(var.workspace) > 0
    error_message = <<EOF
The workspace var is not set: make sure to at least set the WORKSPACE env var.
To fix this you could also set the workspace variable from the arguments, for example:
$ packer build -var=workspace=/src/workspace ...
EOF
  }
}

locals { timestamp = regex_replace(timestamp(), "[- TZ:]", "") }


# source blocks are generated from your builders; a source can be referenced in
# build blocks. A build block runs provisioners and post-processors on a
# source.
source "amazon-ebs" "request_simulation" {
  ami_name      = "request-simulation-${local.timestamp}"
  instance_type = "m5.2xlarge"
  region        = var.regions[0]
  ami_regions   = var.regions
  source_ami_filter {
    filters = {
      name                = "amzn2-ami-kernel-*-x86_64-gp2"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["137112412989"]
  }
  tags = {
    commit_version = var.commit_version
  }
  ssh_username = "ec2-user"
}

# a build block invokes sources and runs provisioning steps on them.
build {
  sources = ["source.amazon-ebs.request_simulation"]

  provisioner "file" {
    source      = join("/", [var.distribution_dir, "/request_simulation_docker_image.tar"])
    destination = "/home/ec2-user/request_simulation_docker_image.tar"
  }
  provisioner "file" {
    source      = join("/", [var.distribution_dir, "aws-otel-collector.rpm"])
    destination = "/home/ec2-user/aws-otel-collector.rpm"
  }
  provisioner "file" {
    source      = join("/", [var.distribution_dir, "otel_collector_config.yaml"])
    destination = "/home/ec2-user/otel_collector_config.yaml"
  }
  provisioner "shell" {
    script = join("/", [var.workspace, "production/packaging/tools/request_simulation/ami/setup.sh"])
  }
}
