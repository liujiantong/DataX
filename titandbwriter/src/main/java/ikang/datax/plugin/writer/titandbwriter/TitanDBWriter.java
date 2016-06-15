package ikang.datax.plugin.writer.titandbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Created by liutao on 16/6/8.
 */
public class TitanDBWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(TitanDBWriter.class);
    private static final String IDX_NAME_FMT = "index_%s_%s";


    public static class Job extends Writer.Job {
        private Configuration originalConfig;
        private Map<String, String> columnTypeMap = new HashMap<>(16);
        private Set<String> indexNameSet = new HashSet<>(16);

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            logger.debug("begin job split");
            List<Configuration> configList = new ArrayList<>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = this.getPluginJobConf();
            validateParameter();
            List<Configuration> columns = this.originalConfig.getListConfiguration(Key.COLUMN);
            for (Configuration c : columns) {
                String cname = c.get(Key.NAME, String.class);
                columnTypeMap.put(cname, c.get(Key.TYPE, String.class));
            }
        }

        private void validateParameter() {
            String titanConf = originalConfig.getNecessaryValue(Key.TITANDB_CONF, TitanDBWriterErrorCode.REQUIRED_VALUE);
            // warn: 这里需要配一个配置文件名
            File dir = new File(titanConf);
            if (!dir.isFile()) {
                throw DataXException.asDataXException(
                        TitanDBWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置的path: [%s] 不是一个合法的配置文件.", titanConf));
            }

            List<Configuration> columns = this.originalConfig.getListConfiguration(Key.COLUMN);
            if (columns == null || columns.isEmpty()) {
                throw DataXException.asDataXException(
                        TitanDBWriterErrorCode.ILLEGAL_VALUE,
                        String.format("[%s] 参数必须设置", com.alibaba.datax.plugin.rdbms.writer.Key.COLUMN));
            }
        }

        @Override
        public void prepare() {
            String titandbConf = this.originalConfig.getString(Key.TITANDB_CONF);
            logger.info("titandbConf: {}", titandbConf);

            TitanGraph graph = TitanFactory.open(titandbConf);
            graph.tx().rollback();

            TitanManagement mgmt = graph.openManagement();
            List<Configuration> verticesConfig = originalConfig.getListConfiguration(Key.GRAPH_VERTICES);
            for (Configuration vconf : verticesConfig) {
                String labelName = vconf.get("label", String.class);
                logger.info("labelName:{}", labelName);

                if (!mgmt.containsVertexLabel(labelName)) {
                    logger.info("=====> Create vertex label:{}", labelName);
                    mgmt.makeVertexLabel(labelName).make();

                    List<Configuration> props = vconf.getListConfiguration("properties");
                    if (props == null) {
                        logger.error("Vertex has no properties");
                        throw DataXException.asDataXException(
                                TitanDBWriterErrorCode.ILLEGAL_VALUE,
                                String.format("[%s] 参数必须设置", Key.PROPERTIES));
                    }

                    logger.debug("Create {} properties for Vertex:{}", props.size(), labelName);

                    for (Configuration pc : props) {
                        String pname = pc.get("name", String.class);
                        if (!mgmt.containsPropertyKey(pname)) {
                            String ptype = columnTypeMap.get(pname);
                            Class pclazz = columnType(ptype);
                            PropertyKey propertyKey = mgmt.makePropertyKey(pname).dataType(pclazz).make();
                            String indexType = pc.getString(Key.INDEX);
                            if (indexType == null) continue;

                            String idxName = String.format(IDX_NAME_FMT, labelName, pname);
                            if (mgmt.containsGraphIndex(idxName)) continue;

                            indexNameSet.add(idxName);

                            TitanManagement.IndexBuilder indexBuilder = mgmt
                                    .buildIndex(idxName, Vertex.class)
                                    .addKey(propertyKey);
                            if ("unique".equals(indexType)) {
                                indexBuilder.unique();
                            }
                            indexBuilder.buildCompositeIndex();
                        }
                    }
                }
            }

            mgmt.commit();
        }

        @Override
        public void post() {
            logger.info("Reindexing graph");
            String titandbConf = this.originalConfig.getString(Key.TITANDB_CONF);
            TitanGraph graph = TitanFactory.open(titandbConf);
            graph.tx().rollback();

            TitanManagement mgmt = graph.openManagement();
            try {
                Iterator<String> itr = indexNameSet.iterator();
                while (itr.hasNext()) {
                    mgmt.updateIndex(mgmt.getGraphIndex(itr.next()), SchemaAction.REINDEX).get();
                }
            } catch (Exception e) {
                logger.error("Reindex graph error: {}", e);
                mgmt.rollback();
                return;
            }
            mgmt.commit();
        }

        @Override
        public void destroy() {

        }

    }


    public static class Task extends Writer.Task {
        private int columnNumber;
        private Configuration writerSliceConfig;
        private Map<String, Integer> columnIndexMap = new HashMap<>(16);
        private Map<String, String> columnTypeMap = new HashMap<>(16);

        @Override
        public void prepare() {
            List<Configuration> columns = writerSliceConfig.getListConfiguration(Key.COLUMN);
            int idx = 0;
            for (Configuration c : columns) {
                String cname = c.get(Key.NAME, String.class);
                columnIndexMap.put(cname, idx++);
                logger.debug("columnIndexMap: {} => {}", cname, idx-1);
                columnTypeMap.put(cname, c.get(Key.TYPE, String.class));
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String titandbConf = this.writerSliceConfig.getString(Key.TITANDB_CONF);

            TitanGraph graph = TitanFactory.open(titandbConf);
            GraphTraversalSource g = graph.traversal();
            TitanTransaction tx = graph.newTransaction();

            List<Configuration> verticesConfig = writerSliceConfig.getListConfiguration(Key.GRAPH_VERTICES);

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                logger.debug("record number: {}", record.getColumnNumber());
                if (record.getColumnNumber() != this.columnNumber) {
                    throw DataXException.asDataXException(
                            TitanDBWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format(
                                    "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                    record.getColumnNumber(),
                                    this.columnNumber));
                }

                for (Configuration vconf : verticesConfig) {
                    String labelName = vconf.get(Key.LABEL, String.class);
                    logger.info("labelName: {}", labelName);

                    // Assert: We check properties before
                    List<Configuration> props = vconf.getListConfiguration(Key.PROPERTIES);
                    boolean isUnique = false;

                    // Check unique property
                    String pname = null;
                    for (Configuration pc : props) {
                        pname = pc.get(Key.NAME, String.class);
                        String index = pc.get(Key.INDEX, String.class);
                        if ("unique".equals(index)) {
                            isUnique = true;
                            break;
                        }
                    }

                    boolean createFlag = true;
                    if (isUnique) {
                        logger.debug("property:{} is unique", pname);
                        int cidx = columnIndexMap.get(pname);
                        Object cval = columnValue(record.getColumn(cidx));
                        if (cval != null && g.V().has(labelName, pname, cval).hasNext()) {
                            logger.info("found property:{} with value:{}", pname, cval);
                            createFlag = false;
                        }
                    }
                    if (!createFlag) continue;

                    TitanVertex vertex = tx.getOrCreateVertexLabel(labelName);
                    for (Configuration pc : props) {
                        String pn = pc.get(Key.NAME, String.class);
                        String cname = pc.get(Key.COLUMN, String.class);
                        logger.info("pn:{}, cname:{}", pn, cname);

                        Integer idx = columnIndexMap.get(cname);
                        if (idx == null) {
                            throw DataXException.asDataXException(
                                    TitanDBWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                    String.format("[%s] 您的参数配置错误", Key.COLUMN));
                        }

                        Column column = record.getColumn(idx);
                        Object cval = columnValue(column);
                        if (cval == null) continue;

                        logger.info("column:{}, cval:{}", column.asString(), cval);
                        if (vertex.query().labels(labelName).has(pn, cval).count() == 0) {
                            logger.info("Create vertex label:{} has property:{}=>{}", labelName, pn, cval);
                            vertex.property(pn, cval);
                        }
                    }
                }
            }

            tx.commit();
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.columnNumber = writerSliceConfig.getListConfiguration(Key.COLUMN).size();
        }

        @Override
        public void destroy() {

        }
    }

    private final static Class columnType(String type) {
        if (Column.Type.STRING.name().equalsIgnoreCase(type)) {
            return String.class;
        } else if (Column.Type.DATE.name().equalsIgnoreCase(type)) {
            return Date.class;
        } else if (Column.Type.DOUBLE.name().equalsIgnoreCase(type)) {
            return Double.class;
        } else if (Column.Type.BOOL.name().equalsIgnoreCase(type)) {
            return Boolean.class;
        } else if (Column.Type.BYTES.name().equalsIgnoreCase(type)) {
            return Byte.class;
        } else {
            return Long.class;
        }
    }

    private final static Object columnValue(Column column) {
        if (column.getRawData() == null) return null;

        switch (column.getType()) {
            case STRING:
                return column.asString();
            case DATE:
                return column.asDate();
            case DOUBLE:
                return column.asDouble();
            case BOOL:
                return column.asBoolean();
            case BYTES:
                return column.asBytes();
            default:
                return column.asLong();
        }
    }

}
