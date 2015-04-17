#!/usr/bin/env python

import os
from os.path import dirname
from os.path import join
import time

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))

host_file_name = "localhost"
java_heap_size = "1g"

# Program parameters
num_worker_threads = 16
staleness = 0
num_iterations = 10

jar_file = join(app_dir, "build", "libs", "hello_ssp-all.jar")
host_file = join(proj_dir, "machinefile", host_file_name)

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

for client_id, ip in enumerate(host_ips):
  print "Running worker", str(client_id), "on", ip
  cmd = ssh_cmd + ip
  cmd += " java -ea -Xms512m -Xmx%s" % java_heap_size
  cmd += " -cp \"" + jar_file + ":" + proj_dir + "\""
  cmd += " org.petuum.app.hello_ssp.HelloSSP"
  cmd += " -clientId " + str(client_id)
  cmd += " -hostFile " + host_file
  cmd += " -numWorkerThreads " + str(num_worker_threads)
  cmd += " -staleness " + str(staleness)
  cmd += " -numIterations " + str(num_iterations)
  cmd += " &"

  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)

