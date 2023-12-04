package org.duckdb;

public class DuckDBColumnTypeMetaData {
    public final short type_size;
    public final short width;
    public final short scale;

    public DuckDBColumnTypeMetaData(short type_size, short width, short scale) {
        this.type_size = type_size;
        this.width = width;
        this.scale = scale;
    }

    public static DuckDBColumnTypeMetaData parseColumnTypeMetadata(String columnTypeDetail) {
        String[] split_details = columnTypeDetail.split(";");
        return new DuckDBColumnTypeMetaData(Short.parseShort(split_details[0].replace("DECIMAL", "")),
                                            Short.parseShort(split_details[1]), Short.parseShort(split_details[2]));
    }
}
