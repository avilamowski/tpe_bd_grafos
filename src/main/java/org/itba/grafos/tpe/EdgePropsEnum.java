package org.itba.grafos.tpe;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public enum EdgePropsEnum {
    LABELE("labelE", DataTypes.StringType, false),
    DIST("dist", DataTypes.IntegerType, true),;

    private final String id;
    private final DataType dataType;
    private final boolean isNullable;

    EdgePropsEnum(String id, DataType dataType, boolean isNullable) {
        this.id = id;
        this.dataType = dataType;
        this.isNullable = isNullable;
    }

    public String getId(){
        return id;
    }

    public StructField toStructField() {
        return DataTypes.createStructField(id, dataType, isNullable);
    }

}
