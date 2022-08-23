package org.duckdb;

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
        return source == "true" || source == "false";
    }
    public boolean isNull() {
        return source == "null";
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
}
