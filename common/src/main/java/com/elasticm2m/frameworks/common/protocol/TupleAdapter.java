package com.elasticm2m.frameworks.common.protocol;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleset.geo.geojson.GeoJsonModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public class TupleAdapter<T> {

    public static final Logger LOG = LoggerFactory.getLogger(TupleAdapter.class);
    public static final String ID_FIELD_NAME = "id";
    public static final String BODY_FIELD_NAME = "body";
    public static final String GROUP_BY_FIELD_NAME = "groupBy";
    public static final String PROPERTIES_FIELD_NAME = "properties";
    private final static Fields FIELDS = new Fields(ID_FIELD_NAME, BODY_FIELD_NAME, PROPERTIES_FIELD_NAME, GROUP_BY_FIELD_NAME);
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new GeoJsonModule())
            .configure(INDENT_OUTPUT, false)
            .setSerializationInclusion(NON_EMPTY);
    private String id;
    private T body;
    private Object rawBody;
    private String groupKey;
    private Map<String, Object> properties = new HashMap<>();
    private Class<T> typeClass;
    private List<Object> values = new ArrayList<>();

    public TupleAdapter(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    public TupleAdapter(Tuple tuple, Class<T> typeClass) {
        this.typeClass = typeClass;
        if (tuple != null && !tuple.getValues().isEmpty()) {
            loadTuple(tuple.getValues());
        }
    }

    public TupleAdapter(List<Object> tuple, Class<T> typeClass) {
        this.typeClass = typeClass;
        if (tuple != null && !tuple.isEmpty()) {
            loadTuple(tuple);
        }
    }

    public static Fields getFields() {
        return FIELDS;
    }

    public <V> V convertValue(Object value, Class<V> typeClass) {
        V result = null;
        try {
            result = objectMapper.convertValue(value, typeClass);
        } catch (Exception ex) {
            LOG.error("Error converting value: ", ex);
        }
        return result;
    }

    public void fromTuple(Tuple tuple) {
        loadTuple(tuple.getValues());
    }

    public void fromTuple(List<Object> tuple) {
        loadTuple(tuple);
    }

    public T getBody() {
        return body;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setBody(T body) {
        this.body = body;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Object getProperty(String name) {
        Object value = null;
        if (properties != null) {
            value = properties.get(name);
        }
        return value;
    }

    public <V> V getProperty(String name, Class<V> propertyClass) {
        Object value = getProperty(name);

        if (value != null) {
            if (propertyClass.isAssignableFrom(value.getClass())) {
                return (V) value;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private void loadTuple(List<Object> tuple) {
        values.clear();
        int size = tuple.size();
        if (size == 1) {
            rawBody = tuple.get(0);
        } else {
            if (size > 1) {
                id = (String) tuple.get(0);
                rawBody = tuple.get(1);
            } else if (size == 3) {
                id = (String) tuple.get(0);
                rawBody = tuple.get(1);
                properties = (Map<String, Object>) tuple.get(2);
            } else if (size == 4) {

            }
        }
            id = (String) tuple.get(0);
        body = readValue(tuple.get(1), typeClass);
        if (tuple.size() > 2) {
            properties = (Map<String, Object>) tuple.get(2);
        }
        if (tuple.size() > 3) {
            groupKey = (String) tuple.get(3);
        }
    }

    public <V> V readValue(Object value, Class<V> typeClass) {
        V result = null;
        try {
            if (value instanceof String) {
                result = objectMapper.readValue((String) value, typeClass);
            } else if (value instanceof byte[]) {
                result = objectMapper.readValue((byte[]) value, typeClass);
            }
        } catch (Exception ex) {
            LOG.error("Error deserializing value: ", ex);
        }
        return result;
    }

    public void setProperty(String name, Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        result.append("TupleAdapter {");
        result.append("id=").append(id).append(", ");
        result.append("properties=").append(writeValue(properties)).append(", ");
        result.append("groupKey=").append(groupKey).append(", ");
        result.append("body=").append(writeValue(body));
        result.append("}");

        return result.toString();
    }

    public List<Object> toTuple() throws TupleSerializationException {
        List<Object> values = new Values();
        values.add(id);
        values.add(writeValue(body));
        values.add(properties);
        values.add(groupKey);

        return values;
    }

    public String writeValue(Object value) {
        String result = null;
        try {
            result = objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            LOG.error("Error serializing value: ", ex);
        }
        return result;
    }
}
