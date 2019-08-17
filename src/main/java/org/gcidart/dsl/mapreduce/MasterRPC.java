package org.gcidart.dsl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.webserver.WebServer;

public class MasterRPC {
	public static void main( String[] args ) throws XmlRpcException, IOException, InterruptedException {
		/* 
		 * args[0] = port
		 * args[1] = jobName
		 * args[2] = Number of Reduce Tasks
		 * args[3...] = filenames
		 */
        Master mstr = new Master(args[1]);
        PropertyHandlerMapping mapping = new PropertyHandlerMapping();
        mapping.setRequestProcessorFactoryFactory(new MasterFactoryFactory(mstr));
        mapping.addHandler( "Master", org.gcidart.dsl.mapreduce.Master.class );
        WebServer server = new WebServer( Integer.parseInt(args[0]) );
        server.getXmlRpcServer().setHandlerMapping( mapping );
        server.start();
        ArrayList<String> filenames = new ArrayList<String>();
		for(int i=3; i < args.length; i++)
        {
			filenames.add(args[i]);
		}
		mstr.runOp(args[1], filenames , Integer.parseInt(args[2]));
		mstr.shutWorkers();
		mstr.mergeReduceFiles();
	}
}
