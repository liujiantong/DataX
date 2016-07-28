package ikang.datax.plugin.writer.titandbwriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.base.Strings;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.EdgeLabelMaker;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;


/**
 * TitanDBWriter 从关系型数据库中读入数据, 生成Titan 图数据库的节点和关系.
 *
 * Created by liutao on 16/6/8.
 */
public class TitanDBWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(TitanDBWriter.class);
    private static final String IDX_NAME_FMT = "index_%s_%s";
    private static final double DEFAULT_WEIGHT = 0.75d;


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

            //TitanManagement mgmt = graph.openManagement();
            List<Configuration> verticesConfig = originalConfig.getListConfiguration(Key.GRAPH_VERTICES);
            for (Configuration vconf : verticesConfig) {
                String labelName = vconf.getString(Key.LABEL);
                logger.debug("labelName:{}", labelName);

                // create vertex labels
                TitanManagement mgmt = graph.openManagement();
                PropertyKey weightKey = mgmt.getPropertyKey(Key.WEIGHT);
                if (weightKey == null) {
                    weightKey = mgmt.makePropertyKey(Key.WEIGHT).dataType(Double.class).make();
                }

                if (!mgmt.containsVertexLabel(labelName)) {
                    logger.debug("=====> Create vertex label:{}", labelName);
                    mgmt.makeVertexLabel(labelName).make();
                }

                List<Configuration> props = vconf.getListConfiguration(Key.PROPERTIES);
                if (props == null) {
                    logger.error("Vertex has no properties");
                    throw DataXException.asDataXException(
                            TitanDBWriterErrorCode.ILLEGAL_VALUE,
                            String.format("[%s] 参数必须设置", Key.PROPERTIES));
                }

                logger.debug("Create {} properties for Vertex:{}", props.size(), labelName);

                for (Configuration pc : props) {
                    String pname = pc.getString(Key.NAME);
                    String idxName = String.format(IDX_NAME_FMT, labelName, pname);

                    if (!mgmt.containsPropertyKey(pname)) {
                        String ptype = columnTypeMap.get(pname);
                        Class pclazz = columnType(ptype);
                        PropertyKey propertyKey = mgmt.makePropertyKey(pname).dataType(pclazz).make();
                        String indexType = pc.getString(Key.INDEX);
                        if (indexType == null) continue;

                        if (!mgmt.containsGraphIndex(idxName)) {
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

                    indexNameSet.add(idxName);
                }

                // create edge labels
                List<Configuration> edges = vconf.getListConfiguration(Key.EDGES);
                if (edges == null || edges.isEmpty()) continue;

                for (Configuration econf : edges) {
                    String eLabel = econf.getString(Key.LABEL);
                    if (!mgmt.containsEdgeLabel(eLabel)) {
                        logger.debug("create edge label:{}", eLabel);
                        EdgeLabelMaker elmaker = mgmt.makeEdgeLabel(eLabel);
                        elmaker.signature(weightKey);
                        String multip = econf.getString(Key.MULTIPLICITY);
                        if (multip != null) {
                            Multiplicity mtype = multiplicity(multip);
                            if (mtype != null) {
                                logger.debug("edge label:{} has multiplicity:{}", eLabel, mtype.name());
                                elmaker.multiplicity(mtype);
                            }
                        }
                        elmaker.make();
                    }
                }
                mgmt.commit();
            }

            graph.close();
        }


        @Override
        public void post() {
            logger.info("Run post job...");

            String titandbConf = this.originalConfig.getString(Key.TITANDB_CONF);
            TitanGraph graph = TitanFactory.open(titandbConf);
            graph.tx().rollback();

            logger.info("Found {} reindex jobs", indexNameSet.size());

            Iterator<String> itr = indexNameSet.iterator();
            while (itr.hasNext()) {
                TitanManagement mgmt = graph.openManagement();
                try {
                    mgmt.updateIndex(mgmt.getGraphIndex(itr.next()), SchemaAction.REINDEX).get();
                } catch (Exception e) {
                    logger.error("Reindex graph error: {}", e);
                    mgmt.rollback();
                    continue;
                }
                mgmt.commit();
            }

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
        private Map<String, Pattern> patterns = new HashMap<>(16);


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

            List<Configuration> verticesConfig = writerSliceConfig.getListConfiguration(Key.GRAPH_VERTICES);
            for (Configuration vconf : verticesConfig) {
                List<Configuration> props = vconf.getListConfiguration(Key.PROPERTIES);
                for (Configuration pc : props) {
                    String regex = pc.getString(Key.PATTERN);
                    if (regex != null) {
                        Pattern pattern = patterns.get(regex);
                        if (pattern == null) {
                            pattern = Pattern.compile(regex);
                            patterns.put(regex, pattern);
                        }
                    }
                }
            }
        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String titandbConf = this.writerSliceConfig.getString(Key.TITANDB_CONF);

            TitanGraph graph = TitanFactory.open(titandbConf);
            //GraphTraversalSource g = graph.traversal();
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

                // we have multiple vertices each record(row)
                // row : vertex : property => 1 : m : m
                NEW_VERTEX:
                for (Configuration vconf : verticesConfig) {
                    String labelName = vconf.getString(Key.LABEL);
                    logger.debug("labelName: {}", labelName);

                    // Assert: We check properties before
                    List<Configuration> props = vconf.getListConfiguration(Key.PROPERTIES);

                    // parse edge config
                    List<Configuration> edges = vconf.getListConfiguration(Key.EDGES);
                    if (edges != null && !edges.isEmpty()) {
                        edgesMap.put(labelName, edges);
                    }

                    Map<String, Object> vprops = new HashMap<>(8);
                    TitanTransaction txVertex = null;
                    Vertex vertex = null;
                    try {
                        NEW_VERTEX_PROPERTY:
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
                            String regex = pc.getString(Key.PATTERN);
                            Boolean required = pc.getBool(Key.REQUIRED);

                            if (cval == null || isColumnNullOrEmpty(column)) {
                                if (Boolean.TRUE.equals(required)) {
                                    continue NEW_VERTEX;
                                }
                                continue NEW_VERTEX_PROPERTY;
                            } else {
                                if (regex != null) {
                                    Pattern pattern = patterns.get(regex);
                                    if (!pattern.matcher(column.asString()).matches()) {
                                        if (Boolean.TRUE.equals(required)) {
                                            continue NEW_VERTEX;
                                        }
                                        continue NEW_VERTEX_PROPERTY;
                                    }
                                }
                            }

                            boolean pcreated = true;
                            String indexType = pc.getString(Key.INDEX);
                            if (Key.INDEX_UNIQUE.equals(indexType)) {
                                GraphTraversalSource g = graph.traversal();
                                GraphTraversal traversal = g.V().hasLabel(labelName).has(pn, cval);

                                // found existing vertex with same property
                                if (traversal.hasNext()) {
                                    pcreated = false;

                                    if (vertex == null) {
                                        vertex = (Vertex) traversal.next();
                                    }

                                    vertexMap.put(labelName, (Long)vertex.id());
                                    logger.debug("v[{}] exists with p[{}] conflicted", vertex.id(), pn);
                                }
                            }

                            if (pcreated) {
                                vprops.put(pn, cval);
                                logger.debug("====> Create vertex label:{} has property:{}=>{}", labelName, pn, column.asString());
                            }
                        } // end of checking all properties of vertex

                        // rollback vertex without property
                        if (vprops.isEmpty()) {
                            continue NEW_VERTEX;
                        }

                        txVertex = graph.newTransaction();
                        if (vertex == null) {
                            vertex = txVertex.addVertex(labelName);
                            vertexMap.put(labelName, (Long)vertex.id());
                        }

                        Iterator<Map.Entry<String, Object>> vpIter = vprops.entrySet().iterator();
                        while (vpIter.hasNext()) {
                            Map.Entry<String, Object> entry = vpIter.next();
                            String pn = entry.getKey();
                            Object cval = entry.getValue();
                            vertex.property(pn, cval);
                        }
                        txVertex.commit();

                        //vertexMap.put(labelName, (Long)vertex.id());
                    } catch (SchemaViolationException e) {
                        // Assertion: never step here
                        if (txVertex != null) txVertex.rollback();
                        logger.warn("Found duplicated vertex:{} with same unique property, error:{}", labelName, e.getMessage());
                        continue NEW_VERTEX;
                    }

                    // this vertex has no edge
                    if (edges == null || edges.isEmpty()) {
                        continue NEW_VERTEX;
                    }
                } // end of all vertices


                // create edges here
                TitanTransaction txEdge = graph.newTransaction();
                Iterator<String> viter = vertexMap.keySet().iterator();
                while (viter.hasNext()) {
                    String label = viter.next();
                    Long vid = vertexMap.get(label);

                    logger.debug("label:{}, vid:{}", label, vid);
                    if (vid == null) continue;

                    List<Configuration> edges = edgesMap.get(label);
                    //logger.debug("edge:{}", edges);
                    if (edges == null) continue;

                    TitanVertex edgeV = txEdge.getVertex(vid);
                    //logger.debug("edgeV:{}", edgeV);
                    if (edgeV == null) continue;

                    for (Configuration ec : edges) {
                        String edgeLabel = ec.getString(Key.LABEL);
                        String vname = ec.getString(Key.VERTEX);
                        double weight = ec.getDouble(Key.WEIGHT, DEFAULT_WEIGHT);

                        Long vid1 = vertexMap.get(vname);
                        if (vid1 == null) continue;

                        TitanVertex v1 = txEdge.getVertex(vid1);
                        if (v1 == null) continue;

                        TitanEdge tedge = edgeV.addEdge(edgeLabel, v1, Key.WEIGHT, weight);
                        logger.debug("Add edge: {} [{}] {}", edgeV, edgeLabel, v1);

                        List<Configuration> psconf = ec.getListConfiguration(Key.PROPERTIES);
                        if (psconf != null && !psconf.isEmpty()) {
                            for (Configuration epc : psconf) {
                                String pname = epc.getString(Key.NAME);
                                String cname = epc.getString(Key.COLUMN);
                                Integer idx = columnIndexMap.get(cname);

                                if (idx == null) continue;

                                Column column = record.getColumn(idx);
                                Object cval = columnValue(column);
                                if (cval != null || !isColumnNullOrEmpty(column)) {
                                    tedge.property(pname, cval);
                                }
                            }
                        }
                    }
                }

                txEdge.commit();
            } // end of all records

            graph.close();
        } // end of startWrite method


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
                String v = column.asString();
                if (v == null) return null;
                return v.trim();
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

    private final static Multiplicity multiplicity(String mtype) {
        if (Multiplicity.MANY2ONE.name().equalsIgnoreCase(mtype)) {
            return Multiplicity.MANY2ONE;
        } else if (Multiplicity.ONE2MANY.name().equalsIgnoreCase(mtype)) {
            return Multiplicity.ONE2MANY;
        } else if (Multiplicity.ONE2ONE.name().equalsIgnoreCase(mtype)) {
            return Multiplicity.ONE2ONE;
        } else if (Multiplicity.SIMPLE.name().equalsIgnoreCase(mtype)) {
            return Multiplicity.SIMPLE;
        } else if (Multiplicity.MULTI.name().equalsIgnoreCase(mtype)) {
            return Multiplicity.MULTI;
        } else {
            return null;
        }
    }

}
