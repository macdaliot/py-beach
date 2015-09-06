include:
  - beach.firewall_updates


build-essential:
  pkg.installed:
    - require:
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 
      

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

python-gevent:
  pkg.installed:
    - require:
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

pyzmq:
  pip.installed:
    - require:
      - pkg: python-pip
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

netifaces:
  pip.installed:
    - require:
      - pkg: python-pip
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

pyyaml:
  pip.installed:
    - require:
      - pkg: python-pip
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

psutil:
  pip.installed:
    - require:
      - pkg: python-pip
      - iptables: open-firewall-updates-https
      - iptables: open-firewall-updates-http
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https 

python-msgpack:
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
    - require_in:
      - iptables: close-firewall-updates-http
      - iptables: close-firewall-updates-https
      