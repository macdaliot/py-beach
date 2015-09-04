firewall-loopback-in:
    iptables.append:
        - table: filter
        - chain: INPUT
        - jump: ACCEPT
        - in-interface: lo
        - save: True

firewall-loopback-out:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - out-interface: lo
        - save: True

define-master:
    host.present:
        - ip: 10.1.1.1
        - name: beach-master

firewall-ntp:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - save: True
        - proto: udp
        - destination: ntp.ubuntu.com
        - dport: 123
    cron.present:
        - name: ntpdate ntp.ubuntu.com
        - hour: 1
        - identifier: time sync

firewall-base-out:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: ESTABLISHED,RELATED
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-base-in:
    iptables.append:
        - table: filter
        - chain: INPUT
        - jump: ACCEPT
        - match: state
        - connstate: ESTABLISHED,RELATED
        - save: True
        - require_in:
            - iptables: firewall-tighten-input

firewall-dns-1:
    iptables.append:
        - name:  "dns-1"
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - save: True
        - proto: udp
        - destination: 8.8.8.8
        - dport: 53
        - require_in:
            - iptables: firewall-tighten-output

firewall-dns-2:
    iptables.append:
        - name:  "dns-2"
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - save: True
        - proto: udp
        - destination: 8.8.4.4
        - dport: 53
        - require_in:
            - iptables: firewall-tighten-output

firewall-tighten-input:
    iptables.set_policy:
        - table: filter
        - chain: INPUT
        - policy: DROP
        - family: ipv4
        - save: True

firewall-tighten-output:
    iptables.set_policy:
        - table: filter
        - chain: OUTPUT
        - policy: DROP
        - family: ipv4
        - save: True
        - require_in:

firewall-cloud-mirror-https:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: mirrors.digitalocean.com
        - dport: 443
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-cloud-mirror-http:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: mirrors.digitalocean.com
        - dport: 80
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-ubuntu-security-https:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: security.ubuntu.com
        - dport: 443
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-ubuntu-security-http:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: security.ubuntu.com
        - dport: 80
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-pypi-https:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: pypi.python.org
        - dport: 443
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-pypi-http:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: pypi.python.org
        - dport: 80
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-cloud-launchpad-https:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: ppa.launchpad.net
        - dport: 443
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-cloud-launchpad-http:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - destination: ppa.launchpad.net
        - dport: 80
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-output

firewall-salt-1:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - dport: 4505
        - proto: tcp
        - save: True
        - destination: beach-master
        - require:
            - host: define-master
        - require_in:
            - iptables: firewall-tighten-output

firewall-salt-2:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - dport: 4506
        - proto: tcp
        - save: True
        - destination: beach-master
        - require:
            - host: define-master
        - require_in:
            - iptables: firewall-tighten-output

firewall-ssh:
    iptables.append:
        - table: filter
        - chain: INPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - dport: 22
        - proto: tcp
        - save: True
        - require_in:
            - iptables: firewall-tighten-input

{% for node_name, node_ip in salt['mine.get']('beach-node-*', 'network.ip_addrs').items()  %}
{% set node_ip = node_ip[ 0 ] %}
beach_node_comms-{{ node_name }}-out:
    iptables.append:
        - table: filter
        - chain: OUTPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - dport: 4999:6000
        - proto: tcp
        - save: True
        - destination: {{ node_ip }}
        - require_in:
                - iptables: firewall-tighten-output
    cmd.run:
        - name: ssh-keyscan {{ node_ip}} >> /root/.ssh/known_hosts

beach_node_comms-{{ node_name }}-in:
    iptables.append:
        - table: filter
        - chain: INPUT
        - jump: ACCEPT
        - match: state
        - connstate: NEW
        - dport: 4999:6000
        - proto: tcp
        - save: True
        - destination: {{ node_ip }}
        - require_in:
            - iptables: firewall-tighten-input
    cmd.run:
        - name: ssh-keyscan {{ node_ip}} >> /root/.ssh/known_hosts

{% endfor %}