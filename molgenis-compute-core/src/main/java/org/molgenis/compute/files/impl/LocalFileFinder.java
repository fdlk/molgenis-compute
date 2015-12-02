package org.molgenis.compute.files.impl;

import java.io.File;

import org.molgenis.compute.files.FileFinder;

public class LocalFileFinder implements FileFinder
{

	@Override
	public File find(String name)
	{
		return new File(name);
	}

}
