package org.molgenis.compute.generators.impl;

import static java.util.stream.Collectors.groupingBy;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.molgenis.compute.data.model.DataEntity;
import org.molgenis.compute.data.model.CollapsedEntities;
import org.molgenis.compute.model.parameters.impl.GlobalParameterImpl;
import org.molgenis.compute.model.parameters.impl.LocalParameterImpl;
import org.molgenis.data.Entity;
import org.molgenis.data.support.MapEntity;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Collapse tuples on targets
 * 
 * @param parameters
 * @param targets
 * @return
 */
public class TupleUtils
{
	private String runID = null;
	private HashMap<String, String> parametersToOverwrite = null;

	/**
	 * Collapses a list of {@link DataEntity} combinations into groups with same values for a set of step inputs.
	 * 
	 * @param parameters
	 *            the {@link DataEntity} combinations to collapse
	 * @param inputs
	 *            the names of the parameters on whose values the combinations should be collapsed
	 * @return the list of {@link CollapsedEntities} values
	 */
	public static List<CollapsedEntities> collapse(List<DataEntity> parameters, List<String> inputs)
	{
		return parameters.stream().collect(groupingBy(p -> getInputValues(inputs, p))).entrySet().stream()
				.map(e -> new LocalParameterImpl(e.getValue(), e.getKey(), inputs)).collect(Collectors.toList());
	}

	/**
	 * Generates a key based on the values of the parameter map
	 * 
	 * @param inputNames
	 * @param parameter
	 * @return The generated key
	 */
	private static String getInputValues(List<String> inputNames, DataEntity parameter)
	{
		String key = "";
		for (String input : inputNames)
		{
			key += parameter.get(input) + "_";
		}
		return key;
	}

	/**
	 * Tuples can have values that are freemarker templates, e.g. ${other column}. This method will solve that
	 * 
	 * @throws IOException
	 * @throws TemplateException
	 */
	public void solve(List<DataEntity> list) throws IOException
	{
		// Freemarker configuration
		@SuppressWarnings("deprecation")
		Configuration freeMarkerConfiguration = new Configuration();
		Template template;

		replaceParameters(list);

		// For every Parameter value
		for (DataEntity parameterValue : list)
		{
			// For every attribute within this parameterValue map
			for (String attribute : parameterValue.getAttributeNames())
			{
				// Store the original
				String original = parameterValue.get(attribute);

				// If the original contains freemarker syntax
				if (original.contains("${"))
				{
					// Check for self reference (??)
					if (original.contains("${" + attribute + "}"))
					{
						throw new IOException("could not solve " + attribute + "='" + original
								+ "' because template references to self");
					}

					// Create a new template for every attribute. Very expensive!!!
					// TODO can we reuse the same template?
					try
					{
						template = freeMarkerConfiguration.getTemplate(attribute);
					}
					catch (IOException e)
					{
						template = new Template(attribute, new StringReader(original), freeMarkerConfiguration);
					}

					StringWriter writer = new StringWriter();
					try
					{
						Map<String, Object> map = parameterValue.toMap();

						// ??
						map.put("runid", runID);

						// Reads the created template, and writes it to a String object.
						template.process(map, writer);
						String value = writer.toString();

						// If the generated template is not the same as it was originally
						if (!value.equals(original))
						{
							parameterValue.set(attribute, value);
						}
					}
					catch (Exception e)
					{
						throw new IOException(
								"could not solve " + attribute + "='" + original + "': " + e.getMessage() + "\n");
					}
				}
			}
		}
	}

	/**
	 * Replaces parameters
	 * 
	 * @param list
	 */
	private void replaceParameters(List<DataEntity> list)
	{
		if (parametersToOverwrite != null)
		{
			for (Map.Entry<String, String> entry : parametersToOverwrite.entrySet())
			{
				String key = entry.getKey();
				String value = entry.getValue();
				for (DataEntity tuple : list)
				{
					tuple.set(key, value);
				}
			}
		}
	}

	/**
	 * Convert a tuple into a map. Columns with a '_' in them will be nested submaps.
	 * 
	 * @param parameterValue
	 * @return A {@link Map} of String Object key value pairs
	 */
	public static Map<String, Object> toMap(Entity parameterValue)
	{
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		for (String attribute : parameterValue.getAttributeNames())
		{
			result.put(attribute, parameterValue.get(attribute));
		}
		return result;
	}

	/**
	 * Uncollapse a tuple using an idColumn
	 * 
	 * @param localParameters
	 * @param idColumn
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<DataEntity> uncollapse(List<CollapsedEntities> localParameters, String idColumn)
	{
		List<DataEntity> result = new ArrayList<DataEntity>();

		for (CollapsedEntities local : localParameters)
		{
			if (!(local.get(idColumn) instanceof List))
			{
				return localParameters;
			}
			else
			{
				for (int i = 0; i < local.getList(idColumn).size(); i++)
				{
					DataEntity copy = new GlobalParameterImpl();
					for (String attribute : local.getAttributeNames())
					{
						if (local.get(attribute) instanceof List)
						{
							copy.set(attribute, local.getList(attribute).get(i));
						}
						else
						{
							copy.set(attribute, local.getString(attribute));
						}
					}
					result.add(copy);
				}
			}
		}

		return result;
	}

	public void setRunID(String runID)
	{
		this.runID = runID;
	}

	public void setParametersToOverwrite(HashMap<String, String> parametersToOverwrite)
	{
		this.parametersToOverwrite = parametersToOverwrite;
	}
}
