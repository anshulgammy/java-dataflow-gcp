package blog.utopian.nerd.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Utility class to provide utility methods for the dataflow. */
public class RedditPostBodyAnalyzerUtil {

  public static List<String> getRedditPostBodyList(String csvFilePath) throws IOException {

    CsvMapper csvMapper = new CsvMapper();

    MappingIterator<List<String>> mappingIterator =
        csvMapper
            .readerForListOf(String.class)
            .with(CsvParser.Feature.WRAP_AS_ARRAY) // !!! IMPORTANT
            .readValues(new FileInputStream(csvFilePath));

    List<String> postBodyList = new ArrayList<>();

    mappingIterator.readAll().stream()
        .skip(1)
        .forEach(
            columnList -> {
              // Post body is present at index 2 in the csv file.
              postBodyList.add(columnList.get(2));
            });

    return postBodyList;
  }
}
