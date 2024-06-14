package it.pagopa.pdnd.nifi.processors.ddbtojson.parser;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import org.apache.nifi.logging.ComponentLog;

import static it.pagopa.pdnd.nifi.processors.ddbtojson.DDBToJson.*;

public class ParserFactory {

    public static Parser createParser(String type, RecordObjectMapper objectMapper, ComponentLog logger) {
        if (KINESIS.toString().equalsIgnoreCase(type)) {
            return new KinesisParser(objectMapper, logger);
        } else if (FULL_EXPORT.toString().equalsIgnoreCase(type)) {
            return new FullExportParser(objectMapper, logger);
        } else if (INCREMENTAL.toString().equalsIgnoreCase(type)) {
            return new IncrementalParser(objectMapper, logger);
        }

        throw new IllegalArgumentException("Invalid parser type: " + type);
    }
}
