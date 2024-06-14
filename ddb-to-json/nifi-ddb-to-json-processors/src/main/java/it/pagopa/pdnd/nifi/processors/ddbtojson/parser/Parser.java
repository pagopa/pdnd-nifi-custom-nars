package it.pagopa.pdnd.nifi.processors.ddbtojson.parser;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import it.pagopa.pdnd.nifi.processors.ddbtojson.model.DynamoConvertedOutputJson;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import java.util.Map;

public abstract class Parser {
    protected static RecordObjectMapper objectMapper;
    protected ComponentLog logger;

    public Parser(RecordObjectMapper objectMapper, ComponentLog logger) {
        Parser.objectMapper = objectMapper;
        this.logger = logger;
    }

    public abstract DynamoConvertedOutputJson parseContent(FlowFile flowFile, String content, Map<String, String> attributes) throws JsonProcessingException;

}
