package org.gcidart.dsl.mapreduce;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.xmlrpc.XmlRpcException;

/* Creates a Worker for Word Count calculation */
public class Worker_wc {
public static void main(String[] args) {
	/*
	 * args[0] - Worker Port
	 * args[1] - Task Limit for Worker
	 * args[2] - Master Port
	 */
		try {
			Worker wkr = new Worker(args[0], Integer.parseInt(args[1]), new WordCountMap(), new WordCountReduce());
			wkr.startServer(Integer.parseInt(args[0]) );
			wkr.register(args[2]);
		}
		catch( MalformedURLException e)
		{
			System.out.println(e);
		}
		catch (XmlRpcException e)
		{
			System.out.println("worker "+e);
		}
		catch( IOException e)
		{
			System.out.println(e);
		}
	}

}
