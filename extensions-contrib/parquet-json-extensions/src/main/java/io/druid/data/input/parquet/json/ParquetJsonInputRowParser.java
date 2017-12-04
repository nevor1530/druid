/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.parquet.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ParquetJsonInputRowParser implements InputRowParser<ObjectNode> {
  static Logger logger = LoggerFactory.getLogger(ParquetJsonInputRowParser.class);
  private final ParseSpec parseSpec;
  private final boolean binaryAsString;
  private final List<String> dimensions;

  private final StringInputRowParser stringInputRowParser;

  @JsonCreator
  public ParquetJsonInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;

    stringInputRowParser = new StringInputRowParser(parseSpec, null);

    List<DimensionSchema> dimensionSchema = parseSpec.getDimensionsSpec().getDimensions();
    this.dimensions = Lists.newArrayList();
    for (DimensionSchema dim : dimensionSchema) {
      this.dimensions.add(dim.getName());
    }
  }

  /**
   */
  public InputRow parse(ObjectNode record)
  {
    String json = record.toString();
    return stringInputRowParser.parse(json);
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetJsonInputRowParser(parseSpec, binaryAsString);
  }
}
