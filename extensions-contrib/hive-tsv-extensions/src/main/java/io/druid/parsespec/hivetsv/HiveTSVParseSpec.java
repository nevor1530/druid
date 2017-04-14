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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.TimestampSpec;

import java.util.List;

/**
 */
public class HiveTSVParseSpec extends DelimitedParseSpec {
    static Logger log = new Logger(HiveTSVParseSpec.class);

    public HiveTSVParseSpec(@JsonProperty("timestampSpec") TimestampSpec timestampSpec, @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec, @JsonProperty("delimiter") String delimiter, @JsonProperty("listDelimiter") String listDelimiter, @JsonProperty("columns") List<String> columns) {
        super(timestampSpec, dimensionsSpec, delimiter, listDelimiter, columns);
    }

    @Override
    public Parser<String, Object> makeParser() {
        log.info("make parser");
        Parser<String, Object> retVal = new HiveTSVParser(
                Optional.fromNullable(getDelimiter()),
                Optional.fromNullable(getListDelimiter())
        );
        retVal.setFieldNames(getColumns());
        log.info("after making parser");
        return retVal;
    }
}
