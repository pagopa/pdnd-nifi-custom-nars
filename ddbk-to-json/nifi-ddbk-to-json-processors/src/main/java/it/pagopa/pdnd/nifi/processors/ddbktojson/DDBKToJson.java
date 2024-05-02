/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.pagopa.pdnd.nifi.processors.ddbktojson;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.pagopa.pdnd.nifi.processors.ddbktojson.model.DDBKRecord;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"DynamoDB", "Convert", "JSON", "Read", "Kinesis"})
@CapabilityDescription("Converts a DynamoDB JSON format in a plain JSON. Saves the root metadata in attributes starting with ddbk_* prefix")
@WritesAttributes({
        @WritesAttribute(attribute = DDBKToJson.DDBK_AWS_REGION, description = "AWS Region saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_EVENT_ID, description = "Event ID saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_EVENT_NAME, description = "Event Name saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_EVENT_SOURCE, description = "Event Source saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_EVENT_VERSION, description = "Event Version saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_PRINCIPAL_ID, description = "If UserIdentity is not null: Principal ID saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_PRINCIPAL_TYPE, description = "If UserIdentity is not null: Principal Type saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_RECORD_FORMAT, description = "Record Format saved on DynamoDB JSON"),
        @WritesAttribute(attribute = DDBKToJson.DDBK_TABLE_NAME, description = "Table name saved on DynamoDB JSON")
})
public class DDBKToJson extends AbstractProcessor {
    public static final String DDBK_PRINCIPAL_TYPE = "ddbk_principal_type";
    public static final String DDBK_PRINCIPAL_ID = "ddbk_principal_id";
    public static final String DDBK_EVENT_VERSION = "ddbk_event_version";
    public static final String DDBK_EVENT_SOURCE = "ddbk_event_source";
    public static final String DDBK_EVENT_NAME = "ddbk_event_name";
    public static final String DDBK_EVENT_ID = "ddbk_event_id";
    public static final String DDBK_AWS_REGION = "ddbk_aws_region";
    public static final String DDBK_RECORD_FORMAT = "ddbk_record_format";
    public static final String DDBK_TABLE_NAME = "ddbk_table_name";
    private static final ObjectMapper MAPPER;

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


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);

        String contentString = extractContent(session, flowFile);

        DDBKRecord deserialized;
        try {
            deserialized = MAPPER.readValue(contentString, DDBKRecord.class);
        } catch (JsonProcessingException e) {
            getLogger().error("Failed to parse input FlowFile {} due to {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (deserialized == null || deserialized.getDynamodb() == null || deserialized.getDynamodb().getNewImage() == null) {
            getLogger().warn("Empty new image for FlowFile {}", flowFile);
            session.transfer(flowFile, REL_WARN);
            return;
        }

        String outJson;
        try {
            outJson = prepareOutputJson(deserialized);
        } catch (JsonProcessingException e) {
            getLogger().error("Failed to add fields to outputJson {} due to {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        FlowFile resultFlowFile = session.write(flowFile, outputStream -> outputStream.write(outJson.getBytes()));

        //TODO Adds other getDynamoDB values?
        final Map<String, String> attributes = new HashMap<>();
        addAttributes(deserialized, attributes);
        resultFlowFile = session.putAllAttributes(resultFlowFile, attributes);

        getLogger().info("Transferred {} to 'success'", flowFile);
        session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(resultFlowFile, REL_SUCCESS);
    }

    private static String prepareOutputJson(DDBKRecord deserialized) throws JsonProcessingException {
        Item ddbItem = ItemUtils.toItem(deserialized.getDynamodb().getNewImage());
        String outJson;
        JsonNode jsonNode = MAPPER.readTree(ddbItem.toJSON());
        outJson = ((ObjectNode) jsonNode)
                        .put("eventID", deserialized.getEventID())
                        .put("eventName", deserialized.getEventName())
                .toString();
        return outJson;
    }

    private static String extractContent(ProcessSession session, FlowFile flowFile) {
        final int flowFileSize = (int) flowFile.getSize();
        final byte[] buffer = new byte[flowFileSize];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer, false));
        return new String(buffer, 0, flowFileSize);
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
    }

    private static void checkNullAndAdd(Map<String, String> attributes, String key, String value) {
        if(StringUtils.isNotEmpty(value)){
            attributes.put(key, value);
        }
    }

}
