package org.itba.grafos.tpe;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public enum VertexPropsEnum {
    LABELV("labelV", DataTypes.StringType, false),
    TYPE("type", DataTypes.StringType, false),
    CODE("code", DataTypes.StringType, true),
    ICAO("icao", DataTypes.StringType, true),
    DESC("desc", DataTypes.StringType, true),
    REGION("region", DataTypes.StringType, true),
    RUNWAYS("runways", DataTypes.IntegerType, true),
    LONGEST("longest", DataTypes.IntegerType, true),
    ELEV("elev", DataTypes.IntegerType, true),
    COUNTRY("country", DataTypes.StringType, true),
    CITY("city", DataTypes.StringType, true),
    LAT("lat", DataTypes.DoubleType, true),
    LON("lon", DataTypes.DoubleType, true),
    AUTHOR("author", DataTypes.StringType, true),
    DATE("date", DataTypes.StringType, true);

    final String id;
    final DataType dataType;
    final boolean isNullable;


    VertexPropsEnum(String id, DataType dataType, boolean isNullable) {
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
