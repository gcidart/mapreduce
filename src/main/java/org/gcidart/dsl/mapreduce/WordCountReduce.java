package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;

/*Reduce Implementation for Word Count*/
class WordCountReduce implements ReduceInterface {

	@Override
	public String reduceFunc(String key, ArrayList<String> values) {
		return Integer.toString(values.size());
	}
	

}
