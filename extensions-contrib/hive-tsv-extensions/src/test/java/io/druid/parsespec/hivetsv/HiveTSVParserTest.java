package io.druid.parsespec.hivetsv;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.parsers.AbstractFlatTextFormatParser;
import io.druid.java.util.common.parsers.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveTSVParserTest
{
  @Test
  public void testWithHeaderRowOfEmptyColumns()
  {
    final Parser<String, Object> parser = new HiveTSVParser("\t", null, false, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(AbstractFlatTextFormatParser.FlatTextFormat.DELIMITED, "hello", "\\N", "foo", "")
    };
    final Map<String, Object> jsonMap = parser.parseToMap(body[0]);
    Assert.assertEquals("null key", true, jsonMap.containsKey("column_2"));
    Assert.assertEquals("null value", null, jsonMap.get("column_2"));
    Assert.assertEquals("null value", null, jsonMap.get("column_4"));
  }

  private static String concat(AbstractFlatTextFormatParser.FlatTextFormat format, String... values)
  {
    return Arrays.stream(values).collect(Collectors.joining(format.getDefaultDelimiter()));
  }

}