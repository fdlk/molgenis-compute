package org.molgenis.compute.files.impl;

import java.io.File;

import org.molgenis.compute.files.FileFinder;
import org.molgenis.compute.urlreader.impl.UrlReaderImpl;

public class WebWorkflowFileFinder implements FileFinder
{
	private final UrlReaderImpl urlReaderImpl = new UrlReaderImpl();
	private final String webWorkflowLocation;

	public WebWorkflowFileFinder(String webWorkflowLocation)
	{
		this.webWorkflowLocation = webWorkflowLocation;
	}

	@Override
	public File find(String fileName)
	{
		return urlReaderImpl.createFileFromGithub(webWorkflowLocation, fileName);
	}

}
