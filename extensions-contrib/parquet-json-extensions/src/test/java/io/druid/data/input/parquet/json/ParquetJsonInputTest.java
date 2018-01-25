package io.druid.data.input.parquet.json;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import org.apache.parquet.json.JsonParquetReadSupport;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by nevor on 04/12/2017.
 */
public class ParquetJsonInputTest {
    @Test
    public void testSchema() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/ingestion_config.json"));
        Configuration conf = new Configuration();
        conf.setBoolean(JsonParquetReadSupport.FETCH_PRIMITIVE_FIELDS, true);
        Job job = Job.getInstance(conf);
        config.intoConfiguration(job);
        ObjectNode data = getFirstRecord(job, "example/-r-00368.snappy.parquet");
        assertEquals(1494222853161L, data.get("start_timestamp").asLong());
        assertEquals(227, data.get("url_num").asInt());
        assertEquals(false, data.get("is_bounce").asBoolean());
        assertEquals("Zhihu", data.get("client").get("product").asText());
        assertEquals(1131, data.get("entry").size());
        // assert auto fetch primitive fields
        assertEquals(1494227189788L, data.get("end_timestamp").asLong());
        assertEquals(4336627, data.get("duration").asInt());
    }

    @Test
    public void testNewerSchema() throws IOException, InterruptedException {
        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/newer_schema_config.json"));
        Job job = Job.getInstance(new Configuration());
        config.intoConfiguration(job);
        ObjectNode data = getFirstRecord(job, "example/zalog.snappy.parquet");
        assertEquals(false, data.get("detail").get("view").get("is_intent").asBoolean());
    }

    /**
     * 测试 Parquet LIST 类型的数据读取，因为暂时没有小份的测试数据文件，测试完去掉了
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testListType() throws IOException, InterruptedException {
//        Job job = Job.getInstance(new Configuration());
//        ObjectNode data = getFirstRecord(job, "example/000000_0");
//        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(data));
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
