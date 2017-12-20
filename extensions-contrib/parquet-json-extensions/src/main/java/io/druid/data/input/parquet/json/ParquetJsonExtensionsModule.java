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
                new SimpleModule("Json2StringInputRowParserModule")
                        .registerSubtypes(
                                new NamedType(Json2StringInputRowParser.class, "json_2_string")
                        )
        );
    }

    @Override
    public void configure(Binder binder) {
    }
}
