package ikang.datax.plugin.writer.titandbwriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.base.Strings;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;


/**
 * TitanDBWriter 从关系型数据库中读入数据, 生成Titan 图数据库的节点和关系.
 *
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
                String cname = c.getString(Key.NAME);
                columnTypeMap.put(cname, c.getString(Key.TYPE));
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
                String labelName = vconf.getString("label");
                logger.debug("labelName:{}", labelName);

                if (!mgmt.containsVertexLabel(labelName)) {
                    logger.debug("=====> Create vertex label:{}", labelName);
                    VertexLabel vertexLabel = mgmt.makeVertexLabel(labelName).make();

                    List<Configuration> props = vconf.getListConfiguration("properties");
                    if (props == null) {
                        logger.error("Vertex has no properties");
                        throw DataXException.asDataXException(
                                TitanDBWriterErrorCode.ILLEGAL_VALUE,
                                String.format("[%s] 参数必须设置", Key.PROPERTIES));
                    }

                    logger.debug("Create {} properties for Vertex:{}", props.size(), labelName);

                    for (Configuration pc : props) {
                        String pname = pc.getString("name");
                        if (!mgmt.containsPropertyKey(pname)) {
                            String ptype = columnTypeMap.get(pname);
                            Class pclazz = columnType(ptype);
                            PropertyKey propertyKey = mgmt.makePropertyKey(pname).dataType(pclazz).make();
                            String indexType = pc.getString(Key.INDEX);
                            if (indexType == null) continue;

                            String idxName = String.format(IDX_NAME_FMT, labelName, pname);
                            if (mgmt.containsGraphIndex(idxName)) continue;
                            indexNameSet.add(idxName);

                            logger.debug("add propertyKey:{} to index:{}", pname, idxName);
                            TitanManagement.IndexBuilder indexBuilder = mgmt
                                    .buildIndex(idxName, Vertex.class)
                                    .addKey(propertyKey);
                            if (Key.INDEX_UNIQUE.equals(indexType)) {
                                indexBuilder.unique();
                            }
                            indexBuilder.buildCompositeIndex();
                        }
                    }
                }
            }

            mgmt.commit();
            graph.close();
        }

        @Override
        public void post() {
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
            graph.close();
            logger.info("Reindex graph done");
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
                String cname = c.getString(Key.NAME);
                columnIndexMap.put(cname, idx++);
                logger.debug("columnIndexMap: {} => {}", cname, idx-1);
                columnTypeMap.put(cname, c.getString(Key.TYPE));
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String titandbConf = this.writerSliceConfig.getString(Key.TITANDB_CONF);

            TitanGraph graph = TitanFactory.open(titandbConf);
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

                Map<String, Long> vertexMap = new HashMap<>(record.getColumnNumber());
                Map<String, List<Configuration>> edgesMap = new HashMap<>(record.getColumnNumber());

                new_vertex:
                for (Configuration vconf : verticesConfig) {
                    String labelName = vconf.getString(Key.LABEL);
                    logger.debug("labelName: {}", labelName);

                    // Assert: We check properties before
                    List<Configuration> props = vconf.getListConfiguration(Key.PROPERTIES);

                    Set<String> propSet = new HashSet<>(8);
                    TitanTransaction tx = graph.newTransaction();
                    try {
                        TitanVertex vertex = tx.addVertex(labelName);
                        new_vertex_property:
                        for (Configuration pc : props) {
                            String pn = pc.getString(Key.NAME);
                            String cname = pc.getString(Key.COLUMN);
                            logger.debug("pn:{}, cname:{}", pn, cname);

                            Integer idx = columnIndexMap.get(cname);
                            if (idx == null) {
                                throw DataXException.asDataXException(
                                        TitanDBWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("[%s] 您的参数配置错误, [%s] 字段不存在",
                                                Key.COLUMN, cname));
                            }

                            Column column = record.getColumn(idx);
                            Object cval = columnValue(column);
                            if (cval == null || isColumnNullOrEmpty(column)) {
                                Boolean required = pc.getBool(Key.REQUIRED);
                                if (Boolean.TRUE.equals(required)) {
                                    tx.rollback();
                                    continue new_vertex;
                                }
                                continue new_vertex_property;
                            }

                            vertex.property(pn, cval);
                            propSet.add(pn);
                            logger.info("====> Create vertex label:{} has property:{}=>{}", labelName, pn, column.asString());
                        }

                        // remove vertex without property
                        if (propSet.isEmpty()) {
                            logger.debug("propSet empty, vertex creation rollback");
                            tx.rollback();
                            continue new_vertex;
                        }
                        vertexMap.put(labelName, (Long)vertex.id());
                    } catch (SchemaViolationException e) {
                        logger.warn("Found duplicated vertex:{} with same unique property", labelName);
                        tx.rollback();
                        continue new_vertex;
                    }
                    tx.commit();

                    // parse edge config
                    List<Configuration> edges = vconf.getListConfiguration(Key.EDGES);
                    if (edges != null && !edges.isEmpty()) {
                        edgesMap.put(labelName, edges);
                    }
                }

                // create edges here
                TitanTransaction tx = graph.newTransaction();
                Iterator<String> vitr = vertexMap.keySet().iterator();
                while (vitr.hasNext()) {
                    String label = vitr.next();
                    Long vid = vertexMap.get(label);
                    if (vid == null) continue;

                    TitanVertex v = tx.getVertex(vid);
                    List<Configuration> edges = edgesMap.get(label);

                    if (v == null || edges == null) continue;

                    logger.debug("vertex:{} has {} edges", label, edges.size());

                    for (Configuration ec : edges) {
                        String edgeLabel = ec.getString(Key.LABEL);
                        String vname = ec.getString("vertex");

                        Long vid1 = vertexMap.get(vname);
                        if (vid1 == null) continue;

                        TitanVertex v1 = tx.getVertex(vid1);
                        if (v1 == null) continue;

                        logger.info("Add edge: {} [{}] {}", v, edgeLabel, v1);
                        v.addEdge(edgeLabel, v1);
                    }
                }
                tx.commit();
            }

            graph.close();
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

    private final static boolean isColumnNullOrEmpty(Column column) {
        if (column.getRawData() == null) return true;

        switch (column.getType()) {
            case STRING:
                return Strings.isNullOrEmpty(column.asString());
            case INT:
            case LONG:
            case DOUBLE:
                return (0 == column.asLong());
            default:
                return false;
        }
    }

}
