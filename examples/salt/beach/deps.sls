build-essential:
  pkg.installed: []

python-pip:
  pkg.installed: []

python-dev:
  pkg.installed: []

python-gevent:
  pkg.installed: []

pyzmq:
  pip.installed:
  - require:
    - pkg: python-pip

netifaces:
  pip.installed:
  - require:
    - pkg: python-pip

pyyaml:
  pip.installed:
  - require:
    - pkg: python-pip

psutil:
  pip.installed:
  - require:
    - pkg: python-pip

python-msgpack:
  pkg.installed: []

beach-pkg:
  pip.installed:
    - name: beach