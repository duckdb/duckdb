package org.duckdb;

import java.util.Objects;

/**
 * Basic wrapper for the JSON type - modelled after the jackson databind JsonNode
 */
public class JsonNode {
    private final String source;
    public JsonNode(String source) {
        this.source = source;
    }
    public boolean isArray() {
        return source.charAt(0) == '[';
    }
    public boolean isObject() {
        return source.charAt(0) == '{';
    }
    public boolean isBoolean() {
        return "true".equals(source) || "false".equals(source);
    }
    public boolean isNull() {
        return "null".equals(source);
    }
    public boolean isNumber() {
        return Character.isDigit(source.charAt(0));
    }
    public boolean isString() {
        return !(isObject() || isBoolean() || isNull() || isArray() || isNumber());
    }
    public String toString() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof JsonNode))
            return false;
        JsonNode jsonNode = (JsonNode) o;
        return Objects.equals(source, jsonNode.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source);
    }
}
