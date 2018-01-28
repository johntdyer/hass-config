#!/usr/bin/env python3
import yaml
from re import match
from sys import argv

with open(argv[2], 'r') as old_file:
  data = yaml.load(old_file)

for device in list(data.keys()):
  if match(argv[1], device):
    del data[device]
    #data[device]['hide_if_away'] = True

with open(argv[3], 'w') as new_file:
  yaml.dump(data, new_file, default_flow_style=False)
