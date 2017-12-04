package io.druid.data.input.parquet.json;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

public class ParquetJsonExtensionsModule implements DruidModule {

    @Override
    public List<? extends Module> getJacksonModules() {
        return Arrays.asList(
                new SimpleModule("ParuqetJsonInputRowParserModule")
                        .registerSubtypes(
                                new NamedType(ParquetJsonInputRowParser.class, "parquet_json_string")
                        )
        );
    }

    @Override
    public void configure(Binder binder) {
    }
}
