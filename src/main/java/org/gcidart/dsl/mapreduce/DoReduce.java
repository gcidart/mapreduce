package org.gcidart.dsl.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.TreeMap;

public class DoReduce {

	DoReduce(String jobName, int reduceTask, String outFile,
			int nMap, ReduceInterface ri) throws FileNotFoundException, IOException
	{
		ArrayList<FileInputStream> in = new ArrayList<FileInputStream>();
		FileOutputStream out = new FileOutputStream(outFile);
		for(int i =0; i <nMap; i++)
		{
			String filename = String.format("%s//mrtmp-%s-%d-%d.txt", System.getProperty("java.io.tmpdir"), jobName, i, reduceTask);
			in.add(new FileInputStream(filename));
		}
		TreeMap<String, ArrayList<String>> keyListValue = new TreeMap<String, ArrayList<String>>();
		for(int i=0; i <nMap; i++)
		{
			BufferedReader br = new BufferedReader( new InputStreamReader(in.get(i)));
			String line ;
			while((line = br.readLine())!=null)
			{
				String[] kvp = line.split(":,:");
				if(keyListValue.containsKey(kvp[0]))
				{
					keyListValue.get(kvp[0]).add(kvp[1]);
				}
				else
				{
					keyListValue.put(kvp[0], new ArrayList<String>());
					keyListValue.get(kvp[0]).add(kvp[1]);
				}
			}
		}
		for(String key:keyListValue.keySet())
		{
			String reduced = ri.reduceFunc(key, keyListValue.get(key));
			String rkv = new String(key+":,:"+reduced+"\n");
			out.write(rkv.getBytes());
		}
		out.flush();
		out.close();
		for(int i=0; i < nMap;i++)
		{
			in.get(i).close();
		}
		
	}
}
