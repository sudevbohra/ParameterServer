#!/usr/bin/env python

import os
from os.path import dirname
from os.path import join
import time

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))

data_file = join(app_dir, "dataset", "netflix.1k1k.entrylist")
exp_name = "netflix_1k1k"
host_file_name = "localhost"
java_heap_size = "4g"

# Program parameters
num_worker_threads = 4
num_minibatches_per_epoch = 5
staleness = 0
num_epochs = 10
K = 100
reg_lambda = 0.05   # lambda is a keyword in python.
learning_rate_decay = 0.995
learning_rate_eta0 = 2.5e-3

jar_file = join(app_dir, "build", "libs", "matrixfact_605-all.jar")
host_file = join(proj_dir, "machinefile", host_file_name)

# Get host IPs
with open(host_file, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]

exp_name += ".E" + str(num_epochs)
exp_name += ".M" + str(num_minibatches_per_epoch)
exp_name += ".N" + str(len(host_ips))
exp_name += ".T" + str(num_worker_threads)
exp_name += ".S" + str(staleness)
exp_name += ".K" + str(K)
exp_name += ".L" + str(reg_lambda)
exp_name += ".D" + str(learning_rate_decay)
exp_name += ".e" + str(learning_rate_eta0)
output_prefix = join(app_dir, "output", exp_name);
os.system("mkdir -p " + dirname(output_prefix))

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
  cmd += " org.petuum.app.matrixfact.MatrixFact"
  cmd += " -clientId " + str(client_id)
  cmd += " -hostFile " + host_file
  cmd += " -numWorkerThreads " + str(num_worker_threads)
  cmd += " -staleness " + str(staleness)
  cmd += " -dataFile " + data_file
  cmd += " -numEpochs " + str(num_epochs)
  cmd += " -numMiniBatchesPerEpoch " + str(num_minibatches_per_epoch)
  cmd += " -K " + str(K)
  cmd += " -lambda " + str(reg_lambda)
  cmd += " -learningRateDecay " + str(learning_rate_decay)
  cmd += " -learningRateEta0 " + str(learning_rate_eta0)
  cmd += " -outputPrefix " + output_prefix
  cmd += " &"

  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)

