beach-usr:
  group.present:
    - name: beach
  user.present:
    - name: beach
    - fullname: Beach
    - groups:
      - beach
    - require:
      - group: beach