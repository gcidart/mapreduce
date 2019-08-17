package org.gcidart.dsl.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;




public class Master {
	
	private ConcurrentLinkedQueue<XmlRpcClient> clients;
	
	private String jobName; //Job name
	private ArrayList<String> files; //List of input files 
	private int nReduce; //Number of reduce partitions
	private int numWorkers;
	
	
	public void mergeReduceFiles()
	{
		try {
			TreeMap<String, String> sKeyValue = new TreeMap<String, String>();
			for(int i=0; i < this.nReduce; i++)
			{
				String filename = String.format("%s//mrtmp-%s-reduce-%d.txt", System.getProperty("java.io.tmpdir"),this.jobName, i);
				FileInputStream in = new FileInputStream(filename);
				BufferedReader br = new BufferedReader( new InputStreamReader(in));
				String line ;
				while((line = br.readLine())!=null)
				{
					String[] kvp = line.split(":,:");
					sKeyValue.put(kvp[0], kvp[1]);
				}
				br.close();
				in.close();
			}
			String outputFilename = String.format("%smrtmp-%s.txt", System.getProperty("java.io.tmpdir"), this.jobName);
			FileOutputStream out = new FileOutputStream(outputFilename);
			for(String key:sKeyValue.keySet())
			{
				String tW = new String(key+" "+sKeyValue.get(key)+"\n");
				out.write(tW.getBytes());
			}
			out.flush();
			out.close();
			System.out.println("Output file: "+outputFilename);
		}
		catch (FileNotFoundException e)
		{
			System.out.println(e);
		}
		catch (IOException e)
		{
			System.out.println(e);
		}
	}
	
	public Master(String masterAddress)
	{
		this.clients = new ConcurrentLinkedQueue<XmlRpcClient>();
		numWorkers = 0;		
	}
	
	public String register(String workerName) throws MalformedURLException, XmlRpcException
	{
			
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		config.setServerURL( new URL( workerName ) );
		XmlRpcClient xrc = new XmlRpcClient();
		xrc.setConfig( config );
		
		Object[] params = { this.numWorkers};        
        String result = ( String ) xrc.execute( "Worker.workerTest", params );
        this.numWorkers++;

        this.clients.add(xrc);
        synchronized (this.clients) {
        	this.clients.notify();
		}
        
        System.out.println( result );
		return "success";
		
	}
	
	public void runOp(String jobName, ArrayList<String> files, Integer nReduce) throws InterruptedException
	{
		this.jobName = jobName;
		this.files = files;
		this.nReduce = nReduce;
		mrJobSchedule("Map"); 
		mrJobSchedule("Reduce"); 
		System.out.println("Map and Reduce done");		
	}
	
	public void shutWorkers() throws XmlRpcException
	{
		for (XmlRpcClient xrc:this.clients )
		{
			this.shutWorker(xrc);
		}
	}
	
	public void shutWorker(XmlRpcClient xrc) throws XmlRpcException
	{
		XmlRpcClientConfigImpl c=(XmlRpcClientConfigImpl) xrc.getClientConfig();
    	
    	Object[] param = {0};
    	int tasks  = ( Integer ) xrc.execute( "Worker.getStats", param );
    	try {
    		xrc.execute( "Worker.stopServer", param );
    	}
    	catch(XmlRpcException e)
    	{
    		System.out.println(c.getServerURL().toString() + " Tasks executed: " + Integer.toString(tasks));
    	}
	}
	
	public void mrJobSchedule(final String jobPhase ) throws InterruptedException 
	{
		int nTasks = nReduce;
		int oTasks = this.files.size();
		if(jobPhase.equals("Map")) 
		{
			nTasks = this.files.size();
			oTasks = nReduce;
		}
		final int foTasks = oTasks;
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		ConcurrentLinkedQueue<Integer> iQ = new ConcurrentLinkedQueue<Integer>();
		ConcurrentLinkedQueue<Integer> rQ = new ConcurrentLinkedQueue<Integer>();
		for(int i =0; i < nTasks; i++)
			iQ.add(i);
		while((rQ.size()!=nTasks))
		{
			if(!iQ.isEmpty())
			{
				int i = iQ.poll();
				executor.submit(() -> {
					Object[] param = { this.jobName, this.files.get(0), jobPhase, i, foTasks};
					if(jobPhase.equals("Map"))
						param[1] = this.files.get(i);
					XmlRpcClient xrc;
					synchronized (clients) {
						while(clients.isEmpty())
						{
							clients.wait();
						}
						System.out.println("Submitted "+jobPhase+" Task "+ Integer.toString(i));
						xrc = clients.poll();
					}
					
					XmlRpcClientConfigImpl c=(XmlRpcClientConfigImpl) xrc.getClientConfig();
					System.out.println("Worker allotted for "+jobPhase+" Task "+
										Integer.toString(i)+": "+c.getServerURL().toString());
					try {
						String result = (String) xrc.execute(xrc.getClientConfig(),"Worker.doTask", param);
						if(result.contentEquals("limitReached"))
						{
							iQ.add(i);
							this.shutWorker(xrc);
						}
						else if(result.contentEquals("success")) {
							System.out.println("Adding Worker back to the Queue after "+
												jobPhase+" Task "+Integer.toString(i));
							clients.add(xrc);
							
							synchronized (this.clients) {
					        	this.clients.notify();
							}
							rQ.add(i);
						}
						else {
							System.out.println("Task failed: "+Integer.toString(i));
							System.out.println("Adding Worker back to the Queue");
							clients.add(xrc);
							
							synchronized (this.clients) {
					        	this.clients.notify();
							}
							rQ.add(i);
						}
							
					}
					catch (XmlRpcException e){
						System.out.println(e);
						/*Request task again*/
						iQ.add(i);
						/*Push worker back to the queue*/
						clients.add(xrc);
					}
				    return null;
				});
			}
			/* Needed for the case when a task is added back to queue after Worker fails to execute the task*/
			else
			{
				Thread.sleep(100);
			}
		}
		
		
		
	}
	
}
