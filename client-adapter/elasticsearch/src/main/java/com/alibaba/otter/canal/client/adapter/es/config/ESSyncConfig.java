package com.alibaba.otter.canal.client.adapter.es.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig implements AdapterConfig {

    private String    dataSourceKey;   // 数据源key

    private String    outerAdapterKey; // adapter key

    private String    groupId;         // group id

    private String    destination;     // canal destination

    private ESMapping esMapping;

    public void validate() {
        if (esMapping._index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if (esMapping._type == null) {
            throw new NullPointerException("esMapping._type");
        }
        if (esMapping._id == null && esMapping.getPk() == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }
        if (esMapping.sql == null) {
            throw new NullPointerException("esMapping.sql");
        }
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public ESMapping getEsMapping() {
        return esMapping;
    }

    public void setEsMapping(ESMapping esMapping) {
        this.esMapping = esMapping;
    }

    public ESMapping getMapping(){
        return esMapping;
    }

    public static class ESMapping implements AdapterMapping{

        private String                       _index;
        private String                       _type;
        private String                       _id;
        private boolean                      upsert          = false;
        private String                       pk;
        private Map<String, RelationMapping> relations       = new LinkedHashMap<>();
        private String                       sql;
        // 对象字段, 例: objFields:
        // - _labels: array:;
        private Map<String, String>          objFields       = new LinkedHashMap<>();
        private List<String>                 skips           = new ArrayList<>();
        private int                          commitBatch     = 1000;
        private String                       etlCondition;
        private boolean                      syncByTimestamp = false;                // 是否按时间戳定时同步
        private Long                         syncInterval;                           // 同步时间间隔

        private SchemaItem                   schemaItem;                             // sql解析结果模型

        private Map<String, NestedMaping> nestedFields = new HashMap<>();                           //nested 类型

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public boolean isUpsert() {
            return upsert;
        }

        public void setUpsert(boolean upsert) {
            this.upsert = upsert;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public Map<String, String> getObjFields() {
            return objFields;
        }

        public void setObjFields(Map<String, String> objFields) {
            this.objFields = objFields;
        }

        public List<String> getSkips() {
            return skips;
        }

        public void setSkips(List<String> skips) {
            this.skips = skips;
        }

        public Map<String, RelationMapping> getRelations() {
            return relations;
        }

        public void setRelations(Map<String, RelationMapping> relations) {
            this.relations = relations;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Long getSyncInterval() {
            return syncInterval;
        }

        public void setSyncInterval(Long syncInterval) {
            this.syncInterval = syncInterval;
        }

        public boolean isSyncByTimestamp() {
            return syncByTimestamp;
        }

        public void setSyncByTimestamp(boolean syncByTimestamp) {
            this.syncByTimestamp = syncByTimestamp;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public Map<String, NestedMaping> getNestedFields() {
            return nestedFields;
        }

        public void setNestedFields(Map<String, NestedMaping> nestedFields) {
            this.nestedFields = nestedFields;
        }

        private Set<String> nestedPks= new HashSet<>();

        public Set<String>  getNestedPks() {
            if(CollectionUtils.isEmpty(nestedFields) || CollectionUtils.isEmpty(nestedFields.values())){
                return null;
            }
            if(!CollectionUtils.isEmpty(nestedPks)){
                return nestedPks;
            }
            nestedFields.values().forEach(nestedMaping -> {
                nestedPks.add(nestedMaping.getPk());
            });
            return nestedPks;
        }
    }

    public static class RelationMapping {

        private String name;
        private String parent;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }
    }
    public static class NestedMaping {
        private String pk;
        private String fields;
        private Set<String> nestedfields = new HashSet<>();
        private boolean valid = false;

        public String getFields() {
            return fields;
        }

        public Set<String> getNestedfields() {
            return nestedfields;
        }

        public void setFields(String fields) {
            String[] split = fields.split(",");
            nestedfields.addAll(Arrays.asList(split));
            setValid(!CollectionUtils.isEmpty(nestedfields));
            this.fields = fields;
        }

        public void setNestedfields(Set<String> nestedfields) {
            this.nestedfields = nestedfields;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public String getPk() {
            return pk;
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }
    }
}
