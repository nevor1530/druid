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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;

import java.util.List;

/**
 */
public class HiveTSVParseSpec extends ParseSpec {
    static Logger log = new Logger(HiveTSVParseSpec.class);

    private final String delimiter;
    private final String listDelimiter;
    private final List<String> columns;

    @JsonCreator
    public HiveTSVParseSpec(
            @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
            @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
            @JsonProperty("delimiter") String delimiter,
            @JsonProperty("listDelimiter") String listDelimiter,
            @JsonProperty("columns") List<String> columns
    )
    {
        super(timestampSpec, dimensionsSpec);

        this.delimiter = delimiter;
        this.listDelimiter = listDelimiter;
        Preconditions.checkNotNull(columns, "columns");
        this.columns = columns;
        for (String column : this.columns) {
            Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
        }

        verify(dimensionsSpec.getDimensions());
    }

    @JsonProperty("delimiter")
    public String getDelimiter()
    {
        return delimiter;
    }

    @JsonProperty("listDelimiter")
    public String getListDelimiter()
    {
        return listDelimiter;
    }

    @JsonProperty("columns")
    public List<String> getColumns()
    {
        return columns;
    }

    @Override
    public void verify(List<String> usedCols)
    {
        for (String columnName : usedCols) {
            Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
        }
    }

    @Override
    public Parser<String, Object> makeParser()
    {
        Parser<String, Object> retVal = new HiveTSVParser(
                Optional.fromNullable(getDelimiter()),
                Optional.fromNullable(getListDelimiter())
        );
        retVal.setFieldNames(getColumns());
        return retVal;
    }

    @Override
    public ParseSpec withTimestampSpec(TimestampSpec spec)
    {
        return new HiveTSVParseSpec(spec, getDimensionsSpec(), delimiter, listDelimiter, columns);
    }

    @Override
    public ParseSpec withDimensionsSpec(DimensionsSpec spec)
    {
        return new HiveTSVParseSpec(getTimestampSpec(), spec, delimiter, listDelimiter, columns);
    }

    public ParseSpec withDelimiter(String delim)
    {
        return new HiveTSVParseSpec(getTimestampSpec(), getDimensionsSpec(), delim, listDelimiter, columns);
    }

    public ParseSpec withListDelimiter(String delim)
    {
        return new HiveTSVParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, delim, columns);
    }

    public ParseSpec withColumns(List<String> cols)
    {
        return new HiveTSVParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, listDelimiter, cols);
    }
}
