package org.molgenis.compute.parsers;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.molgenis.compute.data.model.DataEntities.newDataEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.molgenis.compute.data.model.DataEntities;
import org.molgenis.compute.data.model.DataEntity;
import org.molgenis.compute.data.model.DataSet;
import org.molgenis.compute.data.model.DataSets;
import org.molgenis.compute.data.model.impl.DataSetImpl;
import org.molgenis.data.Entity;
import org.molgenis.data.csv.CsvRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class CsvFileParser
{
	private static final Logger LOG = LoggerFactory.getLogger(CsvFileParser.class);
	private static final Pattern SEQUENCE_PATTERN = Pattern.compile("([+-]?[0-9]+)\\.\\.([+-]?[0-9]+)");

	public CsvFileParser()
	{

	}

	public DataSet parse(File file) throws IOException
	{
		List<DataEntity> entities = new ArrayList<DataEntity>();

		if (file.toString().endsWith(".properties"))
		{
			return new DataSetImpl(singletonList(newDataEntity(parsePropertiesFile(file))));
		}
		else
		{
			// assume we want to parse csv
			if (!file.toString().endsWith(".csv"))
			{
				// assume we want to append '.csv'
				LOG.warn("File '" + file.toString() + "' does not end with *.properties or *.csv.");
				if (file.exists() && file.isFile())
				{
					LOG.info("The file exists. We'll assume it is in the CSV-format and start parsing it...");
				}
				else
				{
					LOG.info("We couldn't find the file. We'll append the extension '.csv' and try again with: "
							+ file.toString() + ".csv");

					file = new File(file.toString() + ".csv");
				}
			}

			for (Entity entity : new CsvRepository(file, null))
			{
				for (DataEntity e : multiplyListAttributes(entity))
				{
					entities.add(e);
				}
			}
		}

		return new DataSetImpl(entities);
	}

	/**
	 * Multiplies all values for all list-valued attributes in this entity.
	 * 
	 * @param entity
	 *            the {@link Entity} to multiply
	 * @return List of {@link Entity}
	 */
	private List<DataEntity> multiplyListAttributes(Entity entity)
	{
		DataSet result = new DataSetImpl(singleton(newDataEntity(entity)));
		for (String attributeName : entity.getAttributeNames())
		{
			List<String> values = asList(entity, attributeName);
			if (values.size() > 1)
			{
				List<DataEntity> entities = values.stream()
						.map(v -> DataEntities.singleValueDataEntity(attributeName, v)).collect(Collectors.toList());
				result = DataSets.productOf(new DataSetImpl(entities), result);
			}
		}

		return newArrayList(result.getEntities());
	}

	/**
	 * Converts an Entity to a list of strings
	 * 
	 * @param entity
	 * @param colName
	 * @return A list of Entity columns
	 */
	private static List<String> asList(Entity entity, String colName)
	{
		String value = entity.getString(colName);
		if (value == null)
		{
			return singletonList("");
		}
		return parseSequence(value).or(entity.getList(colName));
	}

	private static Optional<List<String>> parseSequence(String value)
	{
		Matcher matcher = SEQUENCE_PATTERN.matcher(value);
		// first try as sequence, eg 3..5 (meaning 3, 4, 5)
		if (matcher.find())
		{
			int first = Integer.parseInt(matcher.group(1));
			int second = Integer.parseInt(matcher.group(2));
			int from = Math.min(first, second);
			int to = Math.max(first, second);
			return Optional.of(rangeClosed(from, to).boxed().map(i -> i.toString()).collect(toList()));
		}
		return Optional.absent();
	}

	private Properties parsePropertiesFile(File file) throws FileNotFoundException, IOException
	{
		Properties properties = new Properties();
		FileInputStream fileInputStream = new FileInputStream(file);
		try
		{
			properties.load(fileInputStream);
		}
		catch (Exception e)
		{
			LOG.error("Error loading the file input stream, message is: " + e);
		}
		finally
		{
			fileInputStream.close();
		}

		return properties;
	}
}
