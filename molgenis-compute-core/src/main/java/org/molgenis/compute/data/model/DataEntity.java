package org.molgenis.compute.data.model;

public interface DataEntity
{
	Iterable<String> getAttributeNames();

	String get(String attribute);
}
