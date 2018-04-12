/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.parsespec.hivetsv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.collect.Utils;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.common.parsers.Parser;
import io.druid.java.util.common.parsers.ParserUtils;
import io.druid.java.util.common.parsers.Parsers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class HiveTSVParser implements Parser<String, Object>
{
  private final String delimiter;
  private final Splitter splitter;


  private final String listDelimiter;
  private final Splitter listSplitter;
  private final Function<String, Object> valueFunction;
  private final boolean hasHeaderRow;
  private final int maxSkipHeaderRows;

  private List<String> fieldNames = null;
  private boolean hasParsedHeader = false;
  private int skippedHeaderRows;
  private boolean supportSkipHeaderRows;

  public HiveTSVParser(
      @Nullable final String delimiter,
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this.listDelimiter = listDelimiter != null ? listDelimiter : Parsers.DEFAULT_LIST_DELIMITER;
    this.listSplitter = Splitter.on(this.listDelimiter);
    this.valueFunction = new Function<String, Object>()
    {
      @Nullable
      @Override
      public Object apply(@Nullable String input)
      {
        if (input != null && input.contains(HiveTSVParser.this.listDelimiter)) {
          return StreamSupport.stream(listSplitter.split(input).spliterator(), false)
                              .map(HiveTSVParser::emptyToNull)
                              .collect(Collectors.toList());
        } else {
          return emptyToNull(input);
        }
      }
    };

    this.hasHeaderRow = hasHeaderRow;
    this.maxSkipHeaderRows = maxSkipHeaderRows;
    this.delimiter = delimiter != null ? delimiter : "\t";

    Preconditions.checkState(
        !this.delimiter.equals(getListDelimiter()),
        "Cannot have same delimiter and list delimiter of [%s]",
        this.delimiter
    );

    this.splitter = Splitter.on(this.delimiter);
  }

  public HiveTSVParser(
      @Nullable final String delimiter,
      @Nullable final String listDelimiter,
      final Iterable<String> fieldNames,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this(delimiter, listDelimiter, hasHeaderRow, maxSkipHeaderRows);

    setFieldNames(fieldNames);
  }

  @VisibleForTesting
  HiveTSVParser(@Nullable final String delimiter, @Nullable final String listDelimiter, final String header)
  {
    this(delimiter, listDelimiter, false, 0);

    setFieldNames(header);
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  protected List<String> parseLine(String input) throws IOException
  {
    return splitter.splitToList(input);
  }

  @Override
  public void startFileFromBeginning()
  {
    if (hasHeaderRow) {
      fieldNames = null;
    }
    hasParsedHeader = false;
    skippedHeaderRows = 0;
    supportSkipHeaderRows = true;
  }

  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(final Iterable<String> fieldNames)
  {
    if (fieldNames != null) {
      final List<String> fieldsList = Lists.newArrayList(fieldNames);
      this.fieldNames = new ArrayList<>(fieldsList.size());
      for (int i = 0; i < fieldsList.size(); i++) {
        if (Strings.isNullOrEmpty(fieldsList.get(i))) {
          this.fieldNames.add(ParserUtils.getDefaultColumnName(i));
        } else {
          this.fieldNames.add(fieldsList.get(i));
        }
      }
      ParserUtils.validateFields(this.fieldNames);
    }
  }

  public void setFieldNames(final String header)
  {
    try {
      setFieldNames(parseLine(header));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse header [%s]", header);
    }
  }

  @Override
  public Map<String, Object> parseToMap(final String input)
  {
    if (!supportSkipHeaderRows && (hasHeaderRow || maxSkipHeaderRows > 0)) {
      throw new UnsupportedOperationException("hasHeaderRow or maxSkipHeaderRows is not supported. "
                                              + "Please check the indexTask supports these options.");
    }

    try {
      List<String> values = parseLine(input);

      if (skippedHeaderRows < maxSkipHeaderRows) {
        skippedHeaderRows++;
        return null;
      }

      if (hasHeaderRow && !hasParsedHeader) {
        if (fieldNames == null) {
          setFieldNames(values);
        }
        hasParsedHeader = true;
        return null;
      }

      if (fieldNames == null) {
        setFieldNames(ParserUtils.generateFieldNames(values.size()));
      }

      return Utils.zipMapPartial(fieldNames, Iterables.transform(values, valueFunction));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  public static @Nullable String emptyToNull(@Nullable String string)
  {
    return (string == null || string.length() == 0 || string.equals("\\N")) ? null : string;
  }
}


