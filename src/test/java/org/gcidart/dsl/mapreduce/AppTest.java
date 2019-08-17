package org.gcidart.dsl.mapreduce;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.gcidart.dsl.mapreduce.DoMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
	private final static int nNumber = 100000;
	private final static int mapTasks = 5;
	private final static int reduceTasks = 2;
	private class testMapMethod implements MapInterface
	{
		public ArrayList<KeyValue> mapFunc(String file, String value)
		{
			ArrayList<KeyValue> kvl = new ArrayList<KeyValue>();
			String[] words = value.split("\\s+");
			for(String i:words)
			{
				kvl.add(new KeyValue(i,"_"));
			}
			return kvl;
		}
	}
	private class testReduceMethod implements ReduceInterface
	{
		
		public String reduceFunc(String key, ArrayList<String> values)
		{
			/*for(String v:values)
				System.out.println(v);*/
			return Integer.toString(values.size());
		}
		
	}
	private ArrayList<String> makeInputs(int numFiles) throws IOException, FileNotFoundException
	{
		ArrayList<String> filenames = new ArrayList<String>();
		for(int i=0; i< numFiles; i++)
		{
			filenames.add(new String(String.format("%s//input-test-%d.txt", System.getProperty("java.io.tmpdir"), i)));
			FileOutputStream out = new FileOutputStream(filenames.get(i));
			for (int j=0; j<(i+1)*(nNumber/numFiles); j++) {
				String data = String.format("%d\n", j);
				out.write(data.getBytes());
			}
			out.flush();
			out.close();			
		}
		return filenames;
	}
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    public void checkResult(String jobName) throws NumberFormatException, IOException
    {
    	HashMap<Integer, Integer> tkv = new HashMap<Integer,Integer>();
    	for(int i=0; i< reduceTasks; i++)
        {
        	String reduceFileName = String.format("%s//mrtmp-%s-reduce-%d.txt", System.getProperty("java.io.tmpdir"), jobName, i);
        	FileInputStream fis = new FileInputStream(reduceFileName);
        	BufferedReader br = new BufferedReader( new InputStreamReader(fis));
			String line ;
			while((line = br.readLine())!=null)
			{
				String[] kvp = line.split(":,:");
				int key = Integer.parseInt(kvp[0]);
				assertTrue("range", key>=0);
				assertTrue("range", key<nNumber);
				tkv.put(key,Integer.parseInt(kvp[1]));
			}
			br.close();
					
        }
    	for(int i=0; i < nNumber;i++)
		{
			assertTrue("key found", tkv.containsKey(i));
			int cnt = tkv.get(i);
			int ecnt = mapTasks - i/(nNumber/mapTasks);
			String msg = String.format("%d should occur %d times but seen %d times", i, ecnt, cnt);
			assertTrue(msg, (cnt==ecnt));
		}
    }

   public void testApp() throws IOException, FileNotFoundException
    {
        
    	ArrayList<String> fileNames = makeInputs(mapTasks);
    	String jobName = new String("seqTest");
        for(int i=0; i < mapTasks; i++)
        {
        	new DoMap(jobName, i, fileNames.get(i), reduceTasks, new testMapMethod());
        }
        for(int i=0; i< reduceTasks; i++)
        {
        	String reduceOutputFileName = String.format("%s//mrtmp-%s-reduce-%d.txt", System.getProperty("java.io.tmpdir"), jobName, i);
        	new DoReduce(jobName, i, reduceOutputFileName, mapTasks, new testReduceMethod());
        }
        checkResult(jobName);
    }
}
