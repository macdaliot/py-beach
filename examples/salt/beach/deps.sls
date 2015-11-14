include:
  - beach.firewall_updates


python-pip:
  pkg.installed:
    - require:
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

python-dev:
  pkg.installed:
    - require:
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

beach-pkg:
  pip.installed:
    - name: beach
    - require:
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
      - pkg: python-pip
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https
      