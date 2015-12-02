package org.molgenis.compute.parsers;

import static org.molgenis.compute.data.model.DataSets.productOf;

import java.io.File;
import java.io.IOException;

import org.molgenis.compute.data.model.DataSet;
import org.testng.annotations.Test;

public class CsvFileParserTest
{
	private CsvFileParser parser = new CsvFileParser();

	@Test
	public void testParseLasVegas() throws IOException
	{
		DataSet params = parser.parse(new File("src/main/resources/workflows/lasvegas/parameters.csv"));
		DataSet workflowDefaults = parser
				.parse(new File("src/main/resources/workflows/lasvegas/workflow.defaults.csv"));

		System.out.println(productOf(params, workflowDefaults).getEntities());
	}
}
