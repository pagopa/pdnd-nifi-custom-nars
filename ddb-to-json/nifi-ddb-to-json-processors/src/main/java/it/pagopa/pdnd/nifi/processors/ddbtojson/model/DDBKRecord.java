package it.pagopa.pdnd.nifi.processors.ddbtojson.model;

import com.amazonaws.services.dynamodbv2.model.Record;

public class DDBKRecord extends Record {
    public DDBKRecord() {
    }

    private String tableName;
    private String recordFormat;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRecordFormat() {
        return recordFormat;
    }

    public void setRecordFormat(String recordFormat) {
        this.recordFormat = recordFormat;
    }
}
