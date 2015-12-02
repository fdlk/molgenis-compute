package org.molgenis.compute.data.model;

import java.util.List;
import java.util.Map;

public interface CollapsedEntities
{
	void set(String key, String value);

	void set(String attribute, List<String> list);

	String getString(String key);

	List<String> getList(String attribute);

	Iterable<String> getAttributeNames();

	void addValueToList(String attribute, String value);

	Object get(String attribute);

	// see TupleUtils.toMap
	Map<String, Object> toMap();

	List<Integer> getIntList(String idColumn);

}
