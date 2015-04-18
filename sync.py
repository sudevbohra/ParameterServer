#!/usr/bin/env python

import sys, os, time

if len(sys.argv) != 2:
  print "usage: python prog <hostfile>"
  sys.exit(1)

hostfile = sys.argv[1]

# full path.
sync_path = "~/petuum-java-cmu-605"

with open(hostfile, "r") as f:
  ips = f.read().splitlines()

for ip in ips:
  cmd = "rsync -avhce \"ssh -o StrictHostKeyChecking=no\" " \
      + sync_path + " " + ip.strip() + ":~/"
  print cmd
  time.sleep(2)
  os.system(cmd)
