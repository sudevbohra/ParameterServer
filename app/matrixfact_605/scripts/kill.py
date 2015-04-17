#!/usr/bin/env python

import os, sys

if len(sys.argv) != 2:
  print "usage: ./kill.py <hostfile-path>"
  sys.exit(1)

host_file = sys.argv[1]

# Get host IPs
with open(host_file, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]

ssh_cmd = "ssh -oStrictHostKeyChecking=no" \
  + " -oUserKnownHostsFile=/dev/null -oLogLevel=quiet "

for ip in host_ips:
  cmd = ssh_cmd + ip + " pkill -6 -f org.petuum.app"
  os.system(cmd)
print "Done killing"
