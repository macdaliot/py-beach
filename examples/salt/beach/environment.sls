beach-usr:
  group.present:
    - name: beach
  user.present:
    - name: beach
    - fullname: Beach
    - groups:
      - beach
    - require:
      - group: beach-usr

beach-dir:
  file.directory:
    - name: /beach/code
    - user: beach
    - group: beach
    - mode: 700
    - makedirs: True
    - require:
      - user: beach

beach-config:
  file.managed:
    - name: /beach/beach.yaml
    - source: salt://beach/beach.yaml
    - template: jinja
    - require:
      - file: beach-dir
    - defaults:
      seeds:
{% for node_name, node_ip in salt['mine.get']('beach-node-*', 'network.interfaces').items()  %}
{% set node_ip = node_ip[ 'eth1' ][ 'inet' ][ 0 ][ 'address' ] %}
        - {{ node_ip }}
{% endfor %}
