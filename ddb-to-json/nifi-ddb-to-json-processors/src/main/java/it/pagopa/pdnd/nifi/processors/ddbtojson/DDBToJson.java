package it.pagopa.pdnd.nifi.processors.ddbtojson;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import it.pagopa.pdnd.nifi.processors.ddbtojson.model.DynamoConvertedOutputJson;
import it.pagopa.pdnd.nifi.processors.ddbtojson.parser.Parser;
import it.pagopa.pdnd.nifi.processors.ddbtojson.parser.ParserFactory;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"DynamoDB", "Convert", "JSON", "Read"})
@CapabilityDescription("Converts a DynamoDB JSON format in a plain JSON. Saves the root metadata in attributes starting with ddbk_* prefix")
@WritesAttributes({
        @WritesAttribute(attribute = DDBToJson.DDBK_AWS_REGION, description = "AWS Region saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_EVENT_ID, description = "Event ID saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_EVENT_NAME, description = "Event Name saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_EVENT_SOURCE, description = "Event Source saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_EVENT_VERSION, description = "Event Version saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_PRINCIPAL_ID, description = "If UserIdentity is not null: Principal ID saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_PRINCIPAL_TYPE, description = "If UserIdentity is not null: Principal Type saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_RECORD_FORMAT, description = "Record Format saved on DynamoDB JSON. Only for Kinesis mode."),
        @WritesAttribute(attribute = DDBToJson.DDBK_TABLE_NAME, description = "Table name saved on DynamoDB JSON. Only for Kinesis mode.")
})

public class DDBToJson extends AbstractProcessor {
    public static final String DDBK_PRINCIPAL_TYPE = "ddbk_principal_type";
    public static final String DDBK_PRINCIPAL_ID = "ddbk_principal_id";
    public static final String DDBK_EVENT_VERSION = "ddbk_event_version";
    public static final String DDBK_EVENT_SOURCE = "ddbk_event_source";
    public static final String DDBK_EVENT_NAME = "ddbk_event_name";
    public static final String DDBK_EVENT_ID = "ddbk_event_id";
    public static final String DDBK_AWS_REGION = "ddbk_aws_region";
    public static final String DDBK_RECORD_FORMAT = "ddbk_record_format";
    public static final String DDBK_TABLE_NAME = "ddbk_table_name";
    private static final RecordObjectMapper MAPPER;

    private Parser parser;

    static {
        MAPPER = new RecordObjectMapper();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("Failed processing")
            .name("failure")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("Succeed processing")
            .name("success")
            .build();

    public static final Relationship REL_WARN = new Relationship.Builder()
            .description("Processing with warnings")
            .name("warning")
            .build();

    public static final AllowableValue KINESIS = new AllowableValue("kinesis", "kinesis",
            "Converts a Kinesis DynamoDB JSON");

    public static final AllowableValue FULL_EXPORT = new AllowableValue("full-export", "full export",
            "Converts a DynamoDB JSON exported with a full strategy");

    public static final AllowableValue INCREMENTAL = new AllowableValue("incremental", "incremental",
            "Converts a DynamoDB JSON exported with an incremental strategy");

    public static final PropertyDescriptor PROP_CONVERT = new PropertyDescriptor.Builder()
            .name("input-format")
            .description("Select input format")
            .displayName("Input format")
            .allowableValues(KINESIS, FULL_EXPORT, INCREMENTAL)
            .required(true)
            .defaultValue(KINESIS.getValue())
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(PROP_CONVERT);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_WARN);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if(parser == null) {
            String parserType = context.getProperty(PROP_CONVERT).getValue();
            parser = ParserFactory.createParser(parserType, MAPPER, getLogger());
        }


        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        String contentString = extractContent(session, flowFile);
        String outJson;
        final Map<String, String> attributes = new HashMap<>();

        try {
            DynamoConvertedOutputJson parsedContent = parser.parseContent(flowFile, contentString, attributes);
            if(parsedContent.getFailRelationship() != null){
                //Logged text is inside the parseContent method
                session.transfer(flowFile, parsedContent.getFailRelationship());
                return;
            }
            outJson = parsedContent.getOutputJson();

            FlowFile resultFlowFile = session.write(flowFile, outputStream -> outputStream.write(outJson.getBytes()));

            resultFlowFile = session.putAllAttributes(resultFlowFile, attributes);
            session.getProvenanceReporter().modifyContent(resultFlowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(resultFlowFile, REL_SUCCESS);
            getLogger().info("Transferred {} to 'success'", flowFile);
        } catch (JsonProcessingException e) {
            getLogger().warn("Error processing JSON for FlowFile {}",flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static String extractContent(ProcessSession session, FlowFile flowFile) {
        final int flowFileSize = (int) flowFile.getSize();
        final byte[] buffer = new byte[flowFileSize];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer, false));
        return new String(buffer, 0, flowFileSize);
    }


}
