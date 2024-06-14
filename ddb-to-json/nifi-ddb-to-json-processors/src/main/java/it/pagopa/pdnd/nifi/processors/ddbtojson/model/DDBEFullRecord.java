package it.pagopa.pdnd.nifi.processors.ddbtojson.model;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class DDBEFullRecord {

    @JsonProperty("Item")
    private Map<String, AttributeValue> item;

    public Map<String, AttributeValue> getItem() {
        return item;
    }

    public void setItem(Map<String, AttributeValue> item) {
        this.item = item;
    }
}
