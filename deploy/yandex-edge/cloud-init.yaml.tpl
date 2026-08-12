#cloud-config
ssh_pwauth: false
disable_root: true
users:
  - name: mdh-edge
    system: true
    shell: /usr/sbin/nologin
packages:
  - autossh
  - nginx
write_files:
  - path: /etc/nginx/conf.d/my-data-hub-edge.conf
    owner: root:root
    permissions: '0644'
    encoding: b64
    content: __EDGE_NGINX_CONFIG_B64__
  - path: /etc/nginx/snippets/my-data-hub-proxy.conf
    owner: root:root
    permissions: '0644'
    encoding: b64
    content: __EDGE_PROXY_CONFIG_B64__
  - path: /usr/local/sbin/fetch-my-data-hub-tunnel-key
    owner: root:root
    permissions: '0755'
    encoding: b64
    content: __EDGE_FETCH_KEY_SCRIPT_B64__
  - path: /etc/systemd/system/my-data-hub-edge-key.service
    owner: root:root
    permissions: '0644'
    content: |
      [Unit]
      Description=Fetch my-data-hub edge tunnel key from Lockbox
      After=network-online.target
      Wants=network-online.target
      Before=my-data-hub-edge-tunnel.service

      [Service]
      Type=oneshot
      Environment=MY_DATA_HUB_EDGE_TUNNEL_SECRET_ID=__EDGE_SECRET_ID__
      Environment=MY_DATA_HUB_EDGE_TUNNEL_KEY_FILE=/etc/my-data-hub/tunnel_ed25519
      ExecStart=/usr/local/sbin/fetch-my-data-hub-tunnel-key
      ExecStartPost=/bin/chown mdh-edge:mdh-edge /etc/my-data-hub/tunnel_ed25519
      RemainAfterExit=true
      NoNewPrivileges=true
      PrivateTmp=true
      ProtectHome=true

      [Install]
      WantedBy=multi-user.target
  - path: /etc/systemd/system/my-data-hub-edge-tunnel.service
    owner: root:root
    permissions: '0644'
    encoding: b64
    content: __EDGE_AUTOSSH_SERVICE_B64__
  - path: /etc/my-data-hub/known_hosts
    # write_files runs before the system user from this cloud-config is
    # guaranteed to exist. The host key is public, so keep it root-owned and
    # readable instead of making clean-image initialization order-dependent.
    owner: root:root
    permissions: '0644'
    encoding: b64
    content: __DEVSTAND_KNOWN_HOST_B64__
runcmd:
  - [rm, -f, /etc/nginx/sites-enabled/default]
  - [nginx, -t]
  - [systemctl, daemon-reload]
  - [systemctl, enable, --now, nginx]
  - [systemctl, enable, --now, my-data-hub-edge-key.service]
  - [systemctl, enable, --now, my-data-hub-edge-tunnel.service]
