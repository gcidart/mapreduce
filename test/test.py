import subprocess
import tempfile
arg=""
td = tempfile.gettempdir()

nNumber= 10000000
numFiles= 10

for i in range(numFiles):
	sa = "{}/input-test-{}.txt"
	filename = sa.format(td,i)
	arg = arg + filename + " "
	with open(filename, 'w') as a_writer:
		for j in range((i+1)*(nNumber//numFiles)):
				data = "{}\n".format( j);
				a_writer.write(data);
			
print(arg)
numReduceTasks = 2
masterPort = 8090
jobName = "test"
masterCom = "start java -jar ../target/master.jar-jar-with-dependencies.jar {} {}  {} {}"
subprocess.call(masterCom.format(masterPort, jobName, numReduceTasks, arg), shell=True)

numWorkers = 3
tla = [2, 10, 3]
for i in range(numWorkers):
	workerPort = masterPort + i + 1
	tl = tla[i]
	workerCom = "start java -jar ../target/worker_wc.jar-jar-with-dependencies.jar {} {} {} " 
	subprocess.call(workerCom.format(workerPort, tl, masterPort), shell=True)
