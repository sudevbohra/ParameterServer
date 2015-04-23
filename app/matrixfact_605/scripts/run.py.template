#!/usr/bin/env python

import os
from os.path import dirname
from os.path import join
import time

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))

data_file = join(app_dir, "dataset", "netflix.1k1k.entrylist")
exp_name = "netflix_1k1k"
output_prefix = join(app_dir, "output", exp_name);
host_file_name = "localhost"
java_heap_size = "4g"

# Program parameters
params = {
    "numWorkerThreads": 4
    , "numMiniBatchesPerEpoch": 5
    , "staleness": 0
    , "numEpochs": 10
    , "K": 100
    , "lambda": 0.05
    , "learningRateDecay": 0.995
    , "learningRateEta0": 2.5e-4
    , "outputPrefix": output_prefix
    , "dataFile": data_file
  }

jar_file = join(app_dir, "build", "libs", "matrixfact_605-all.jar")
host_file = join(proj_dir, "machinefile", host_file_name)

# Get host IPs
with open(host_file, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]

exp_name += ".E" + str(params["numEpochs"])
exp_name += ".M" + str(params["numMiniBatchesPerEpoch"])
exp_name += ".N" + str(len(host_ips))
exp_name += ".T" + str(params["numWorkerThreads"])
exp_name += ".S" + str(params["staleness"])
exp_name += ".K" + str(params["K"])
exp_name += ".L" + str(params["lambda"])
exp_name += ".D" + str(params["learningRateDecay"])
exp_name += ".e" + str(params["learningRateEta0"])
os.system("mkdir -p " + dirname(params["outputPrefix"]))

ssh_cmd = (
    "ssh "
    #" -i ~/.ssh/aws_virginia.pem "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    #"-oLogLevel=quiet "
    )

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
  cmd += "".join([" -%s %s" % (k,v) for k,v in params.items()])
  cmd += " &"

  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)

