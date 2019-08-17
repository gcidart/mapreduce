package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;


public interface MapInterface {
	ArrayList<KeyValue> mapFunc(String file, String value);

}
