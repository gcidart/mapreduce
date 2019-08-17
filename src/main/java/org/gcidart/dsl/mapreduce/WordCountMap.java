package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;

/*Map Implementation for Word Count*/
class WordCountMap implements MapInterface {

	@Override
	public ArrayList<KeyValue> mapFunc(String file, String value) {
			ArrayList<KeyValue> kvl = new ArrayList<KeyValue>();
			String[] words = value.split("(\\s|[^a-zA-Z0-9])+");
			for(String i:words)
			{
				kvl.add(new KeyValue(i,"_"));
			}
			return kvl;
		}

}
