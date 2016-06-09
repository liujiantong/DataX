package ikang.datax.plugin.writer.titandbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liutao on 16/6/8.
 */
public class TitanDBWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(TitanDBWriter.class);
    private static int BATCH_SIZE = 1024;

    public static class Job extends Writer.Job {
        private Configuration writerSliceConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            logger.info("begin job split");
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.writerSliceConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            validateParameter();
            String titandbConf = this.writerSliceConfig.getString(Key.TITANDB_CONF);
            logger.info("titandbConf: {}", titandbConf);
        }

        private void validateParameter() {
            String path = writerSliceConfig.getNecessaryValue(Key.TITANDB_CONF, TitanDBWriterErrorCode.REQUIRED_VALUE);
            // warn: 这里需要配一个配置文件名
            File dir = new File(path);
            if (!dir.isFile()) {
                throw DataXException.asDataXException(
                        TitanDBWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的path: [%s] 不是一个合法的配置文件.", path));
            }
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Writer.Task {
        private int batchSize;

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record;
            List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            while((record = lineReceiver.getFromReader()) != null) {
                logger.info("record number: {}", record.getColumnNumber());
                for (int i=0; i<record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    logger.info("column type: {}, value: {}", column.getType(), column.asString());
                }

                /*
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    writerBuffer.clear();
                }
                */
            }
        }

        @Override
        public void init() {
            this.batchSize = BATCH_SIZE;
        }

        @Override
        public void destroy() {

        }
    }

}
