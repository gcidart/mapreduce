package org.gcidart.dsl.mapreduce;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class DoMap {

	DoMap(String jobName, int mapTask, String inFile,
			int nReduce, MapInterface mi)
	{
		try
		{
			String contents = new String(Files.readAllBytes(Paths.get(inFile)));
			
			ArrayList<KeyValue> mkv = mi.mapFunc(inFile, contents);
			ArrayList<FileOutputStream> out = new ArrayList<FileOutputStream>();
			for(int i=0; i < nReduce; i++)
			{
				String filename = String.format("%s//mrtmp-%s-%d-%d.txt", System.getProperty("java.io.tmpdir"), jobName, mapTask, i);
				out.add(new FileOutputStream(filename));
			}
			
			for(KeyValue kv:mkv)
			{
				int bin = (kv.key.hashCode()& 0xfffffff)%nReduce;
				String temp = new String(kv.key+":,:"+kv.value+"\n");
				out.get(bin).write(temp.getBytes());
			}
			for(int i =0; i < nReduce; i++)
			{
				out.get(i).flush();
				out.get(i).close();
			}
		} catch(Exception e) {
			System.out.println(e);
		}
		
	}
}
