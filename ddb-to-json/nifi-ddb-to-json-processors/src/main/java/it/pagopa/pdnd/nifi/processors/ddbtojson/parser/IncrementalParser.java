package it.pagopa.pdnd.nifi.processors.ddbtojson.parser;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import it.pagopa.pdnd.nifi.processors.ddbtojson.model.DynamoConvertedOutputJson;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

import java.util.Map;

import static it.pagopa.pdnd.nifi.processors.ddbtojson.DDBToJson.REL_WARN;

public class IncrementalParser extends Parser {
    public IncrementalParser(RecordObjectMapper objectMapper, ComponentLog logger) {
        super(objectMapper, logger);
    }

    @Override
    public DynamoConvertedOutputJson parseContent(FlowFile flowFile, String content, Map<String, String> attributes) throws JsonProcessingException {
        StreamRecord deserialized = objectMapper.readValue(content, StreamRecord.class);
        if (deserialized == null || deserialized.getNewImage() == null) {
            logger.warn("Empty new image for FlowFile {}", flowFile);
            return new DynamoConvertedOutputJson.Builder()
                    .withFailRelationship(REL_WARN).build();
        }
        return new DynamoConvertedOutputJson.Builder()
                .withOutputJson(prepareOutputJson(deserialized)).build();
    }

    private String prepareOutputJson(StreamRecord deserialized) throws JsonProcessingException {
        Item ddbItem = ItemUtils.toItem(deserialized.getNewImage());
        JsonNode jsonNode = objectMapper.readTree(ddbItem.toJSON());
        return jsonNode.toString();
    }
}
