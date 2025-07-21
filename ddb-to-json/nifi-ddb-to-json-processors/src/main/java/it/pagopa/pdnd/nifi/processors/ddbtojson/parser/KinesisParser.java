package it.pagopa.pdnd.nifi.processors.ddbtojson.parser;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.pagopa.pdnd.nifi.processors.ddbtojson.model.DDBKRecord;
import it.pagopa.pdnd.nifi.processors.ddbtojson.model.DynamoConvertedOutputJson;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;

import java.util.Map;

import static it.pagopa.pdnd.nifi.processors.ddbtojson.DDBToJson.*;

public class KinesisParser extends Parser{
    public KinesisParser(RecordObjectMapper objectMapper, ComponentLog logger) {
        super(objectMapper, logger);
    }

    @Override
    public DynamoConvertedOutputJson parseContent(FlowFile flowFile, String content, Map<String, String> attributes) throws JsonProcessingException {
        DDBKRecord deserialized = objectMapper.readValue(content, DDBKRecord.class);
        //if (deserialized == null || deserialized.getDynamodb() == null || deserialized.getDynamodb().getNewImage() == null) {
        if (deserialized == null || deserialized.getDynamodb() == null || ( deserialized.getDynamodb().getNewImage() == null && !deserialized.getEventName().equals("REMOVE")) || ( deserialized.getDynamodb().getOldImage() == null && deserialized.getEventName().equals("REMOVE"))  ) {  // NEW 

            logger.warn("Empty new image for FlowFile {}", flowFile);
            return new DynamoConvertedOutputJson.Builder()
                    .withFailRelationship(REL_WARN).build();
        }
        addAttributes(deserialized, attributes);
        return new DynamoConvertedOutputJson.Builder()
                .withOutputJson(prepareOutputJson(deserialized)).build();
    }

    private void addAttributes(DDBKRecord record, Map<String, String> attributes) {
        if (record == null) {
            return;
        }

        checkNullAndAdd(attributes, DDBK_AWS_REGION, record.getAwsRegion());
        checkNullAndAdd(attributes, DDBK_EVENT_ID, record.getEventID());
        checkNullAndAdd(attributes, DDBK_EVENT_NAME, record.getEventName());
        checkNullAndAdd(attributes, DDBK_EVENT_SOURCE, record.getEventSource());
        checkNullAndAdd(attributes, DDBK_EVENT_VERSION, record.getEventVersion());
        checkNullAndAdd(attributes, DDBK_RECORD_FORMAT, record.getRecordFormat());
        checkNullAndAdd(attributes, DDBK_TABLE_NAME, record.getTableName());
        if (record.getUserIdentity() != null) {
            checkNullAndAdd(attributes, DDBK_PRINCIPAL_ID, record.getUserIdentity().getPrincipalId());
            checkNullAndAdd(attributes, DDBK_PRINCIPAL_TYPE, record.getUserIdentity().getType());
        }
        if (record.getDynamodb() != null) {
            checkNullAndAdd(attributes, DDBK_APPROXIMATE_CREATION_DATETIME, (Long.toString(record.getDynamodb().getApproximateCreationDateTime().toInstant().toEpochMilli())));
        }
    }

    private void checkNullAndAdd(Map<String, String> attributes, String key, String value) {
        if(StringUtils.isNotEmpty(value)){
            attributes.put(key, value);
        }
    }
    /* 
    private String prepareOutputJson(DDBKRecord deserialized) throws JsonProcessingException {
        Item ddbItem = ItemUtils.toItem(deserialized.getDynamodb().getNewImage());
        ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(ddbItem.toJSON());
        jsonNode.put("eventID", deserialized.getEventID())
                .put("eventName", deserialized.getEventName());
        return jsonNode.toString();
    }
    */

    private String prepareOutputJson(DDBKRecord deserialized) throws JsonProcessingException {
        String eventName = deserialized.getEventName();
        Item ddbItem;
        if(eventName.equals("REMOVE")) {
            ddbItem = ItemUtils.toItem(deserialized.getDynamodb().getOldImage());
        } else {
            ddbItem = ItemUtils.toItem(deserialized.getDynamodb().getNewImage());
        }
        ObjectNode jsonNode = (ObjectNode) objectMapper.readTree(ddbItem.toJSON());
        jsonNode.put("eventID", deserialized.getEventID())
                .put("eventName", deserialized.getEventName())
                .put("approximateCreationDatetime", (Long.toString(deserialized.getDynamodb().getApproximateCreationDateTime().toInstant().toEpochMilli())));

        return jsonNode.toString();
    }

}
