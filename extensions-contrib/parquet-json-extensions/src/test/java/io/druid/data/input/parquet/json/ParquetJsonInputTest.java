package io.druid.data.input.parquet.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by nevor on 04/12/2017.
 */
public class ParquetJsonInputTest {
    @Test
    public void test() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/ingestion_config.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        ObjectNode data = getFirstRecord(job, "example/-r-00368.snappy.parquet");
        assertEquals(1494222853161L, data.get("start_timestamp").asLong());
        assertEquals(227, data.get("url_num").asInt());
        assertEquals(false, data.get("is_bounce").asBoolean());
        assertEquals("Zhihu", data.get("client").get("product").asText());
        assertEquals(1131, data.get("entry").size());
    }

    @Test
    public void testNewerSchema() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/newer_schema_config.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        ObjectNode data = getFirstRecord(job, "example/zalog.snappy.parquet");
        int i = 1;
        assertEquals(false, data.get("detail").get("view").get("is_intent").asBoolean());
    }

    private ObjectNode getFirstRecord(Job job, String parquetPath) throws IOException, InterruptedException {
        File testFile = new File(parquetPath);
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        JsonParquetInputFormat inputFormat = ReflectionUtils.newInstance(JsonParquetInputFormat.class, job.getConfiguration());
        TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, context);

        reader.initialize(split, context);
        reader.nextKeyValue();
        ObjectNode data = (ObjectNode) reader.getCurrentValue();
        reader.close();
        return data;
    }
}
