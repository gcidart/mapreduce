package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;

public interface ReduceInterface {
	String reduceFunc(String key, ArrayList<String> values);

}
