package org.gcidart.dsl.mapreduce;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.XmlRpcRequest;
import org.apache.xmlrpc.server.RequestProcessorFactoryFactory;

public class WorkerFactoryFactory implements RequestProcessorFactoryFactory {
	private final RequestProcessorFactory factory = new WorkerFactory();
	private final Worker wkr;
	public WorkerFactoryFactory(Worker wkr)
	{
		this.wkr = wkr;
	}
	public RequestProcessorFactory getRequestProcessorFactory(@SuppressWarnings("rawtypes") Class aClass)
	         throws XmlRpcException {
	      return factory;
	}
	private class WorkerFactory implements RequestProcessorFactory {
	      public Object getRequestProcessor(XmlRpcRequest xmlRpcRequest)
	          throws XmlRpcException {
	        return wkr;
	      }
	}
	
}
