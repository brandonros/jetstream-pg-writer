terraform {
  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "~> 2.19"
    }
  }
}

provider "vultr" {
  # VULTR_API_KEY comes from env var
}

locals {
  install_docker        = true
  deploy_app            = true
  ssh_authorized_key = file("~/.ssh/id_rsa.pub")
  # echo "hihaters" | openssl passwd -6 -salt ovSvGqIVXC9lTasZ -in -
  user_password_hash = "$6$ovSvGqIVXC9lTasZ$T3YJyx/ew41tndVvqPCV3xZ6tpGTQyQJNXfn/mQ7s9xfvjUy.1g2xLccyW9CattET53xi9Z4REzoNY7iO3Bhw1"
}

resource "vultr_instance" "server1" {
  #plan   = "vc2-1c-1gb"      # 1 vCPU, 1 GB
  plan   = "vc2-2c-4gb"       # 2 vCPUs, 4 GB
  #plan   = "vhf-4c-16gb"     # 4 vCPUs, 16 GB
  #plan   = "voc-c-8c-16gb-150s-amd" # CPU Optimized Cloud, 8 vCPUs, 16 GB
  region = "atl"
  os_id  = 2136               # bookworm
  hostname = "server1"
  user_data = templatefile("${path.module}/../cloud-config.yaml.tpl", {
    install_docker        = local.install_docker
    deploy_app            = local.deploy_app
    ssh_authorized_key = local.ssh_authorized_key
    user_password_hash = local.user_password_hash
  })
}

output "server_id" {
  value = vultr_instance.server1.id
}

output "server_ipv4" {
  value = vultr_instance.server1.main_ip
}
