package org.gcidart.dsl.mapreduce;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.XmlRpcRequest;
import org.apache.xmlrpc.server.RequestProcessorFactoryFactory;

/* https://ws.apache.org/xmlrpc/handlerCreation.html */
public class MasterFactoryFactory implements RequestProcessorFactoryFactory {
	private final RequestProcessorFactory factory = new MasterFactory();
	private final Master mstr;
	public MasterFactoryFactory(Master mstr)
	{
		this.mstr = mstr;
	}
	public RequestProcessorFactory getRequestProcessorFactory(@SuppressWarnings("rawtypes") Class aClass)
	         throws XmlRpcException {
	      return factory;
	}
	private class MasterFactory implements RequestProcessorFactory {
	      public Object getRequestProcessor(XmlRpcRequest xmlRpcRequest)
	          throws XmlRpcException {
	        return mstr;
	      }
	}
	
}
