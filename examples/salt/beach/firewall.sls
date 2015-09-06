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
        - ip: 10.132.124.33
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

{% for node_name, node_ip in salt['mine.get']('beach-node-*', 'network.interfaces').items()  %}
{% set node_ip = node_ip[ 'eth1' ][ 'inet' ][ 0 ][ 'address' ] %}
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