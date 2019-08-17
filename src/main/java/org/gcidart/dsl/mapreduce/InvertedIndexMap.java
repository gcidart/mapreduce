package org.gcidart.dsl.mapreduce;

import java.util.ArrayList;
import java.util.HashSet;

/*Map Implementation for Inverted Index*/
class InvertedIndexMap implements MapInterface {

	@Override
	public ArrayList<KeyValue> mapFunc(String file, String value) {
			ArrayList<KeyValue> kvl = new ArrayList<KeyValue>();
			HashSet<String> seen = new HashSet<String>();
			String[] words = value.split("(\\s|[^a-zA-Z0-9])+");
			for(String i:words)
			{
				if(!seen.contains(i))
				{
					kvl.add(new KeyValue(i,file));
					seen.add(i);
				}
			}
			return kvl;
		}

}