#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

[[ "${1:-}" == "PROVISION_MY_DATA_HUB_YANDEX_EDGE" ]] || {
  echo "usage: $0 PROVISION_MY_DATA_HUB_YANDEX_EDGE" >&2
  exit 2
}
for command_name in yc python3; do
  command -v "$command_name" >/dev/null || { echo "$command_name is required" >&2; exit 2; }
done

folder_id="${MY_DATA_HUB_YC_FOLDER_ID:-b1g5tck18cgqtjb7rn3s}"
zone_id="${MY_DATA_HUB_YC_EDGE_ZONE:-ru-central1-d}"
certificate_id="${MY_DATA_HUB_YC_EDGE_CERTIFICATE_ID:?managed certificate ID is required}"
secret_id="${MY_DATA_HUB_YC_EDGE_TUNNEL_SECRET_ID:?Lockbox tunnel secret ID is required}"
known_host_file="${MY_DATA_HUB_DEVSTAND_KNOWN_HOST_FILE:?pinned devstand known-host file is required}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
state_dir="${MY_DATA_HUB_YC_EDGE_STATE_DIR:-$HOME/.local/state/my-data-hub-yandex-edge}"
[[ "$known_host_file" = /* && -f "$known_host_file" && ! -L "$known_host_file" ]] || {
  echo "known-host input must be an absolute regular non-symlink file" >&2
  exit 2
}
mkdir -p "$state_dir"
chmod 700 "$state_dir"

json_id_by_name() {
  local name="$1"; shift
  "$@" --folder-id "$folder_id" --format json | python3 -c \
    'import json,sys; n=sys.argv[1]; a=[x["id"] for x in json.load(sys.stdin) if x.get("name")==n]; print(a[0] if len(a)==1 else "")' "$name"
}
require_single_id() {
  local value="$1" label="$2"
  [[ "$value" =~ ^[a-z0-9]{20}$ ]] || { echo "$label could not be resolved exactly" >&2; exit 75; }
}

certificate_json="$(yc certificate-manager certificate get --id "$certificate_id" --format json)"
python3 - "$certificate_json" <<'PY'
import json,sys
x=json.loads(sys.argv[1])
if x.get('status') != 'ISSUED': raise SystemExit('managed certificate is not ISSUED')
if set(x.get('domains',())) != {'mcp-datahub.kenigevents.ru','identity.kenigevents.ru'}:
    raise SystemExit('managed certificate has the wrong exact domain set')
PY

network_id="$(json_id_by_name my-data-hub-edge yc vpc network list)"
if [[ -z "$network_id" ]]; then
  network_id="$(yc vpc network create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Isolated network for my-data-hub public TLS edge' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$network_id" network

gateway_id="$(json_id_by_name my-data-hub-edge-nat yc vpc gateway list)"
if [[ -z "$gateway_id" ]]; then
  gateway_id="$(yc vpc gateway create --folder-id "$folder_id" --name my-data-hub-edge-nat \
    --description 'Outbound-only NAT for private my-data-hub edge VM' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$gateway_id" NAT-gateway

route_table_id="$(json_id_by_name my-data-hub-edge-routes yc vpc route-table list)"
if [[ -z "$route_table_id" ]]; then
  route_table_id="$(yc vpc route-table create --folder-id "$folder_id" --name my-data-hub-edge-routes \
    --network-id "$network_id" --route "destination=0.0.0.0/0,gateway-id=$gateway_id" \
    --description 'Private edge outbound route only' --labels project=my-data-hub,scope=public-edge \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$route_table_id" route-table

subnet_id="$(json_id_by_name my-data-hub-edge-d yc vpc subnet list)"
if [[ -z "$subnet_id" ]]; then
  subnet_id="$(yc vpc subnet create --folder-id "$folder_id" --name my-data-hub-edge-d \
    --network-id "$network_id" --zone "$zone_id" --range 10.210.0.0/24 \
    --route-table-id "$route_table_id" --description 'Private ALB/backend subnet' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$subnet_id" subnet

edge_sg_id="$(json_id_by_name my-data-hub-edge-vm-sg yc vpc security-group list)"
if [[ -z "$edge_sg_id" ]]; then
  edge_sg_id="$(yc vpc security-group create --folder-id "$folder_id" --name my-data-hub-edge-vm-sg \
    --network-id "$network_id" --description 'Private edge VM: ALB ingress and bounded egress' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
alb_sg_id="$(json_id_by_name my-data-hub-alb-sg yc vpc security-group list)"
if [[ -z "$alb_sg_id" ]]; then
  alb_sg_id="$(yc vpc security-group create --folder-id "$folder_id" --name my-data-hub-alb-sg \
    --network-id "$network_id" --description 'Public TLS ALB only' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$edge_sg_id" edge-security-group
require_single_id "$alb_sg_id" ALB-security-group

edge_rule_count="$(yc vpc security-group get --id "$edge_sg_id" --format json | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rules",[])))')"
alb_rule_count="$(yc vpc security-group get --id "$alb_sg_id" --format json | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("rules",[])))')"
if [[ "$edge_rule_count" == 0 ]]; then
  yc vpc security-group update-rules --id "$edge_sg_id" \
    --add-rule 'description=ALB-to-nginx,direction=ingress,port=8080,protocol=tcp,v4-cidrs=10.210.0.0/24' \
    --add-rule 'description=ALB-healthcheck,direction=ingress,port=8080,protocol=tcp,predefined=loadbalancer_healthchecks' \
    --add-rule 'description=restricted-SSH-tunnel,direction=egress,port=22,protocol=tcp,v4-cidrs=188.227.84.107/32' \
    --add-rule 'description=HTTPS-APIs,direction=egress,port=443,protocol=tcp,v4-cidrs=0.0.0.0/0' \
    --add-rule 'description=HTTP-packages-metadata,direction=egress,port=80,protocol=tcp,v4-cidrs=0.0.0.0/0' \
    --add-rule 'description=subnet-DNS-UDP,direction=egress,port=53,protocol=udp,v4-cidrs=10.210.0.2/32' \
    --add-rule 'description=subnet-DNS-TCP,direction=egress,port=53,protocol=tcp,v4-cidrs=10.210.0.2/32' >/dev/null
elif [[ "$edge_rule_count" != 7 ]]; then
  echo "existing edge security group differs from the exact seven-rule contract" >&2; exit 75
fi
if [[ "$alb_rule_count" == 0 ]]; then
  yc vpc security-group update-rules --id "$alb_sg_id" \
    --add-rule 'description=public-TLS,direction=ingress,port=443,protocol=tcp,v4-cidrs=0.0.0.0/0' \
    --add-rule 'description=ALB-healthcheck-control,direction=ingress,port=30080,protocol=tcp,predefined=loadbalancer_healthchecks' \
    --add-rule "description=ALB-to-edge,direction=egress,port=8080,protocol=tcp,security-group-id=$edge_sg_id" >/dev/null
elif [[ "$alb_rule_count" != 3 ]]; then
  echo "existing ALB security group differs from the exact three-rule contract" >&2; exit 75
fi

service_account_id="$(json_id_by_name my-data-hub-edge yc iam service-account list)"
if [[ -z "$service_account_id" ]]; then
  service_account_id="$(yc iam service-account create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Read one task-owned Lockbox tunnel key from the private edge VM' \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$service_account_id" edge-service-account
if ! yc lockbox secret list-access-bindings --id "$secret_id" --format json | python3 -c \
  'import json,sys; s=sys.argv[1]; raise SystemExit(0 if any(x.get("role_id")=="lockbox.payloadViewer" and x.get("subject",{}).get("id")==s for x in json.load(sys.stdin)) else 1)' "$service_account_id"; then
  yc lockbox secret add-access-binding --id "$secret_id" --role lockbox.payloadViewer \
    --service-account-id "$service_account_id" >/dev/null
fi

cloud_init="$state_dir/cloud-init.yaml"
python3 "$script_dir/render_cloud_init.py" --secret-id "$secret_id" \
  --known-host-file "$known_host_file" --output "$cloud_init"

instance_id="$(json_id_by_name my-data-hub-edge yc compute instance list)"
if [[ -z "$instance_id" ]]; then
  image_id="$(yc compute image get-latest-from-family ubuntu-2404-lts --folder-id standard-images \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
  instance_id="$(yc compute instance create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Private metadata-only reverse tunnel and nginx edge' --zone "$zone_id" \
    --platform standard-v3 --cores 2 --memory 1G --core-fraction 20 \
    --create-boot-disk "name=my-data-hub-edge,size=10,type=network-hdd,image-id=$image_id,auto-delete=true" \
    --network-interface "subnet-id=$subnet_id,ipv4-address=10.210.0.10,security-group-ids=$edge_sg_id" \
    --service-account-id "$service_account_id" \
    --metadata-from-file "user-data=$cloud_init" \
    --metadata-options gce-http-endpoint=enabled,gce-http-token=enabled,aws-v1-http-endpoint=enabled,aws-v1-http-token=disabled,aws-v2-http-endpoint=disabled,aws-v2-http-token=disabled \
    --labels project=my-data-hub,scope=public-edge --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
require_single_id "$instance_id" edge-instance

target_group_id="$(json_id_by_name my-data-hub-edge yc alb target-group list)"
if [[ -z "$target_group_id" ]]; then
  target_group_id="$(yc alb target-group create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Single private my-data-hub edge target' \
    --target "subnet-id=$subnet_id,ip-address=10.210.0.10" --labels project=my-data-hub,scope=public-edge \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
backend_group_id="$(json_id_by_name my-data-hub-edge yc alb backend-group list)"
if [[ -z "$backend_group_id" ]]; then
  backend_group_id="$(yc alb backend-group create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'HTTP to private nginx edge' --labels project=my-data-hub,scope=public-edge \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
  yc alb backend-group add-http-backend --backend-group-id "$backend_group_id" --name edge-nginx \
    --weight 1 --port 8080 --target-group-id "$target_group_id" \
    --http-healthcheck 'port=8080,healthy-threshold=2,unhealthy-threshold=2,timeout=2s,interval=5s,path=/healthz,expected-statuses=200' >/dev/null
fi

router_id="$(json_id_by_name my-data-hub-edge yc alb http-router list)"
if [[ -z "$router_id" ]]; then
  router_id="$(yc alb http-router create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Exact MCP and OAuth authorities' --labels project=my-data-hub,scope=public-edge \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
  yc alb virtual-host create edge-authorities --http-router-id "$router_id" \
    --authority mcp-datahub.kenigevents.ru,identity.kenigevents.ru >/dev/null
  yc alb virtual-host append-http-route edge-all --http-router-id "$router_id" \
    --virtual-host-name edge-authorities --prefix-path-match / --backend-group-id "$backend_group_id" \
    --request-timeout 300s --request-idle-timeout 300s >/dev/null
fi

address_id="$(json_id_by_name my-data-hub-edge-ip yc vpc address list)"
if [[ -z "$address_id" ]]; then
  address_id="$(yc vpc address create --folder-id "$folder_id" --name my-data-hub-edge-ip \
    --external-ipv4 "zone=$zone_id" --description 'Stable public ALB IPv4 for my-data-hub' \
    --labels project=my-data-hub,scope=public-edge --deletion-protection \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
public_ip="$(yc vpc address get --id "$address_id" --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["external_ipv4_address"]["address"])')"
[[ "$public_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || { echo "reserved public IPv4 missing" >&2; exit 75; }

load_balancer_id="$(json_id_by_name my-data-hub-edge yc alb load-balancer list)"
if [[ -z "$load_balancer_id" ]]; then
  load_balancer_id="$(yc alb load-balancer create --folder-id "$folder_id" --name my-data-hub-edge \
    --description 'Public 443 only for MCP and OAuth' --network-id "$network_id" \
    --security-group-id "$alb_sg_id" --location "subnet-id=$subnet_id,zone=$zone_id" \
    --log-group-use-default --labels project=my-data-hub,scope=public-edge \
    --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
  yc alb load-balancer add-listener --id "$load_balancer_id" --listener-name public-tls \
    --enable-tls --certificate-id "$certificate_id" --external-ipv4-endpoint "port=443,address=$public_ip" \
    --http-router-id "$router_id" --rewrite-request-id >/dev/null
fi

for hostname in mcp-datahub.kenigevents.ru. identity.kenigevents.ru.; do
  if ! yc dns zone list-records --id dnsbhbtvj0l1lf8jpefb --format json | python3 -c \
    'import json,sys; h=sys.argv[1]; ip=sys.argv[2]; raise SystemExit(0 if any(x.get("name")==h and x.get("type")=="A" and x.get("data")==[ip] for x in json.load(sys.stdin).get("record_sets",[])) else 1)' "$hostname" "$public_ip"; then
    yc dns zone add-records --id dnsbhbtvj0l1lf8jpefb --record "$hostname 300 A $public_ip" >/dev/null
  fi
done

receipt="$state_dir/provisioned.env"
cat > "$receipt" <<EOF
certificate_id=$certificate_id
network_id=$network_id
subnet_id=$subnet_id
edge_security_group_id=$edge_sg_id
alb_security_group_id=$alb_sg_id
service_account_id=$service_account_id
instance_id=$instance_id
target_group_id=$target_group_id
backend_group_id=$backend_group_id
router_id=$router_id
address_id=$address_id
public_ip=$public_ip
load_balancer_id=$load_balancer_id
EOF
chmod 600 "$receipt"
printf 'provisioned=true\npublic_ip=%s\nreceipt=%s\n' "$public_ip" "$receipt"
