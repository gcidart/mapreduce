package org.gcidart.dsl.mapreduce;

import java.io.IOException;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;



public class Worker {
	
	private String name;
	private MapInterface mapMethod;
	private ReduceInterface reduceMethod;
	private int tasksLimit; //  RPC tasks limit for this worker
	private int nTasks;
	private int concurrent;
	private WebServer server;
	
	Worker(String name, int tasksLimit, MapInterface mapMethod, ReduceInterface reduceMethod) 
			throws MalformedURLException, XmlRpcException{
		this.name = String.format("http://127.0.0.1:%s", name);
		this.tasksLimit = tasksLimit;
		this.nTasks  = 0;
		this.mapMethod = mapMethod;
		this.reduceMethod = reduceMethod;
    }
	
	/* Register with Master */
	void register(String port) throws MalformedURLException, XmlRpcException{
		XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
		String urla = String.format("http://127.0.0.1:%s", port);
		config.setServerURL( new URL(urla) );

        XmlRpcClient client = new XmlRpcClient();
        client.setConfig( config );

        Object[] params = { this.name };
        String result = ( String ) client.execute( "Master.register", params );

        System.out.println( result );
	}
	
	/* Start the RPC server for this worker */
	void startServer(int port) throws XmlRpcException, IOException
	{
		PropertyHandlerMapping mapping = new PropertyHandlerMapping();
		mapping.setVoidMethodEnabled(true);
		mapping.setRequestProcessorFactoryFactory(new WorkerFactoryFactory(this));
        mapping.addHandler( "Worker", org.gcidart.dsl.mapreduce.Worker.class );
        this.server = new WebServer( port );
        server.getXmlRpcServer().setHandlerMapping( mapping );
        XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) server.getXmlRpcServer().getConfig();
        serverConfig.setEnabledForExtensions(true);
        serverConfig.setContentLengthOptional(false);
        this.server.start();
	}
	
	public int getStats(int dummy)
	{
		return this.nTasks;
	}
	
	/* Shutdown the RPC server for this worker */
	public int stopServer(int dummy)
	{
		this.server.shutdown();
		return this.nTasks;
	}
	
	/* Function for RPC call by Master to perform Map or Reduce operation */
	public String doTask(String jobName, String fileName,  String jobPhase ,
			int taskNumber, int numOtherPhase) throws Exception
	{
		if(this.nTasks >= this.tasksLimit)
			return "limitReached";
		this.nTasks++;
		this.concurrent++;
		int nc = this.concurrent;		
		String taskDetail = String.format("Worker:%s jobName:%s fileName:%s jobPhase:%s taskNumber:%d", 
								this.name, jobName, fileName, jobPhase, taskNumber);
		System.out.println(taskDetail);
		
		if(nc > 1)
			throw new Exception("Can't assign more than one task at a time");
		
		if(jobPhase.equals("Map"))  
		{
			new DoMap(jobName, taskNumber, fileName, numOtherPhase, this.mapMethod);
		}
		else
		{
			String reduceFileName = String.format("%s//mrtmp-%s-reduce-%d.txt", System.getProperty("java.io.tmpdir"), jobName, taskNumber);
			new DoReduce(jobName, taskNumber, reduceFileName, numOtherPhase, this.reduceMethod);
		}
		this.concurrent--;
		
		return "success";
	}
	
	public String workerTest(int workerListSize)
	{
		System.out.println("Worker No. "+ Integer.toString(workerListSize));
		return "success@workerTest";
	}
	
	
	
	
	
	

}
