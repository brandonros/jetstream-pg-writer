#cloud-config
package_update: true

packages:
  - curl
  - ufw
  - git
  - unattended-upgrades
  - fail2ban
  - needrestart
  - apt-transport-https
  - ca-certificates
  - gnupg-agent
  - software-properties-common

write_files:
  - path: /etc/fail2ban/jail.d/sshd-local.conf
    owner: root:root
    permissions: "0644"
    content: |
      [sshd]
      enabled = true
      port = ssh
      filter = sshd
      maxretry = 5
      bantime = 1h
  - path: /etc/needrestart/conf.d/auto-restart.conf
    owner: root:root
    permissions: "0644"
    content: |
      $nrconf{restart} = 'a';

users:
  - name: user
    gecos: "User"
    sudo: ALL=(ALL) NOPASSWD:ALL
    groups: sudo
    shell: /bin/bash
    lock_passwd: false
    passwd: ${user_password_hash}
    ssh_authorized_keys:
      - ${ssh_authorized_key}

runcmd:
  - [ sh, -c, "systemctl enable --now unattended-upgrades" ]
  - [ sh, -c, "systemctl enable --now fail2ban" ]

%{ if install_docker ~}
  # Remove old docker packages (ignore errors if not present)
  - [ sh, -c, "dpkg --remove --force-all docker docker-engine docker.io containerd runc || true" ]
  
  # Add Docker GPG key and repo
  - [ sh, -c, "install -m 0755 -d /etc/apt/keyrings" ]
  - [ sh, -c, "curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc" ]
  - [ sh, -c, "chmod a+r /etc/apt/keyrings/docker.asc" ]
  - [ sh, -c, "echo \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(lsb_release -cs) stable\" > /etc/apt/sources.list.d/docker.list" ]
  
  # Install Docker
  - [ sh, -c, "apt-get update" ]
  - [ sh, -c, "apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin" ]
  - [ sh, -c, "systemctl enable --now docker" ]
  - [ sh, -c, "usermod -aG docker user" ]
%{ endif ~}

%{ if deploy_app ~}
  - [ sh, -c, "git clone https://github.com/brandonros/jetstream-pg-writer.git /home/user/jetstream-pg-writer" ]
  - [ sh, -c, "chown -R user:user /home/user/jetstream-pg-writer" ]
  - [ sh, -c, "install -m 644 /home/user/jetstream-pg-writer/jetstream-pg-writer.service /etc/systemd/system/" ]
  - [ sh, -c, "systemctl daemon-reload" ]
  - [ sh, -c, "systemctl enable --now jetstream-pg-writer" ]
%{ endif ~}

  - [ sh, -c, "ufw default deny incoming" ]
  - [ sh, -c, "ufw default allow outgoing" ]
  - [ sh, -c, "ufw allow OpenSSH" ]
  - [ sh, -c, "ufw --force enable" ]