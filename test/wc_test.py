import subprocess
import os
import re

pattern = "pg-.*\.txt"
filenames = [f for f in os.listdir('.') if re.match(pattern,f)]
dir_path = os.path.dirname(os.path.realpath(__file__))
arg=""
for f in filenames:
	sa = dir_path+"\\"+f+" "
	arg = arg + sa
print(arg)
numReduceTasks = 2
masterPort = 8090
jobName = "wc"
masterCom = "start java -jar ../target/master.jar-jar-with-dependencies.jar {} {}  {} {}"
subprocess.call(masterCom.format(masterPort, jobName, numReduceTasks, arg), shell=True)

numWorkers = 2
tla = [9, 9]
for i in range(numWorkers):
	workerPort = masterPort + i + 1
	tl = tla[i]
	workerCom = "start java -jar ../target/worker_wc.jar-jar-with-dependencies.jar {} {} {} " 
	subprocess.call(workerCom.format(workerPort, tl, masterPort), shell=True)
