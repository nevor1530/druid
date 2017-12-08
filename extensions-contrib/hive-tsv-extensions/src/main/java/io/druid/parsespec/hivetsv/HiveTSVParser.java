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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.collect.Utils;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;
import com.metamx.common.parsers.Parsers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveTSVParser implements Parser<String, Object> {
    private static final Logger log = new Logger(HiveTSVParser.class);

    private static final String DEFAULT_DELIMITER = "\t";

    private final String delimiter;
    private final String listDelimiter;

    private final Splitter splitter;
    private final Splitter listSplitter;
    private final Function<String, Object> valueFunction;

    private ArrayList<String> fieldNames = null;

    public static final Function<String, String> nullEmptyStringFunction = new Function<String, String>() {
        @Override
        public String apply(String input) {
            if (input == null || input.isEmpty() || input.equals("\\N")) {
                return null;
            }
            return input;
        }
    };

    public HiveTSVParser(final Optional<String> delimiter, Optional<String> listDelimiter) {
        this.delimiter = delimiter.isPresent() ? delimiter.get() : DEFAULT_DELIMITER;
        this.listDelimiter = listDelimiter.isPresent() ? listDelimiter.get() : Parsers.DEFAULT_LIST_DELIMITER;

        Preconditions.checkState(
                !this.delimiter.equals(this.listDelimiter),
                "Cannot have same delimiter and list delimiter of [%s]",
                this.delimiter
        );

        this.splitter = Splitter.on(this.delimiter);
        this.listSplitter = Splitter.on(this.listDelimiter);
        this.valueFunction = new Function<String, Object>() {
            @Override
            public Object apply(String input) {
                if (input.contains(HiveTSVParser.this.listDelimiter)) {
                    return Lists.newArrayList(
                            Iterables.transform(
                                    listSplitter.split(input),
                                    nullEmptyStringFunction
                            )
                    );
                } else {
                    return nullEmptyStringFunction.apply(input);
                }
            }
        };
    }

    public HiveTSVParser(
            final Optional<String> delimiter,
            final Optional<String> listDelimiter,
            final Iterable<String> fieldNames
    ) {
        this(delimiter, listDelimiter);

        setFieldNames(fieldNames);
    }

    public HiveTSVParser(final Optional<String> delimiter, final Optional<String> listDelimiter, final String header)

    {
        this(delimiter, listDelimiter);

        setFieldNames(header);
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getListDelimiter() {
        return listDelimiter;
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public void setFieldNames(final Iterable<String> fieldNames) {
        ParserUtils.validateFields(fieldNames);
        this.fieldNames = Lists.newArrayList(fieldNames);
    }

    public void setFieldNames(String header) {
        try {
            setFieldNames(splitter.split(header));
        } catch (Exception e) {
            throw new ParseException(e, "Unable to parse header [%s]", header);
        }
    }

    @Override
    public Map<String, Object> parse(final String input) {
        try {
            Iterable<String> values = splitter.split(input);

            if (fieldNames == null) {
                setFieldNames(ParserUtils.generateFieldNames(Iterators.size(values.iterator())));
            }

            return Utils.zipMapPartial(fieldNames, Iterables.transform(values, valueFunction));
        } catch (Exception e) {
            throw new ParseException(e, "Unable to parse row [%s]", input);
        }
    }
}
