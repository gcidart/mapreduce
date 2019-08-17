package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;

/*Reduce Implementation for Inverted Index*/
class InvertedIndexReduce implements ReduceInterface {

	@Override
	public String reduceFunc(String key, ArrayList<String> values) {
		
		
		String rList =  Integer.toString(values.size());
		for(String filenames: values)
		{
			rList = rList.concat(", "+filenames);
		}
		return rList;
	}
	

}

