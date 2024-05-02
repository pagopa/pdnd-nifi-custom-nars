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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DDBKToJsonTest {

    private TestRunner testRunner;
    private ObjectMapper mapper;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(DDBKToJson.class);
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void testFullInput() throws JsonProcessingException {
        String content = "{\"awsRegion\":\"eu-south-1\",\"eventID\":\"aa69e85c-9587-462f-85bb-8af820ae3b93\",\"eventName\":\"INSERT\",\"userIdentity\":null,\"recordFormat\":\"application/json\",\"tableName\":\"pn-Notifications\",\"dynamodb\":{\"ApproximateCreationDateTime\":1709033076283,\"Keys\":{\"iun\":{\"S\":\"NGTQ-NDUQ-ENGE-202402-N-1\"}},\"NewImage\":{\"requestId\":{\"S\":\"pn-cons-000~PREPARE_ANALOG_DOMICILE.IUN_WNUN-RVXQ-UKET-202312-A-1.RECINDEX_0.ATTEMPT_0.PCRETRY_0\"},\"clientRequestTimeStamp\":{\"S\":\"2023-12-25T19:08:42.053286098Z\"},\"eventsList\":{\"L\":[{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"NULL\":true},\"clientRequestTimeStamp\":{\"NULL\":true},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"NULL\":true},\"registeredLetterCode\":{\"NULL\":true},\"status\":{\"S\":\"booked\"},\"statusCode\":{\"NULL\":true},\"statusDateTime\":{\"S\":\"2023-12-25T19:08:42Z\"},\"statusDescription\":{\"NULL\":true}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"NULL\":true},\"clientRequestTimeStamp\":{\"NULL\":true},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"NULL\":true},\"registeredLetterCode\":{\"NULL\":true},\"status\":{\"S\":\"sent\"},\"statusCode\":{\"S\":\"P000\"},\"statusDateTime\":{\"S\":\"2023-12-25T19:10Z\"},\"statusDescription\":{\"NULL\":true}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-22T13:44:49.249Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON080\"},\"statusCode\":{\"S\":\"CON080\"},\"statusDateTime\":{\"S\":\"2024-01-22T13:15:50Z\"},\"statusDescription\":{\"S\":\"Stampato ed Imbustato\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-22T13:44:49.249Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON010\"},\"statusCode\":{\"S\":\"CON010\"},\"statusDateTime\":{\"S\":\"2024-01-22T13:15:50Z\"},\"statusDescription\":{\"S\":\"Distinta Elettronica inviata a Recapitista\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[{\"M\":{\"date\":{\"S\":\"2024-01-22T14:03:09Z\"},\"documentType\":{\"S\":\"OK Distinta Elettronica da Recapitista\"},\"id\":{\"S\":\"0\"},\"sha256\":{\"S\":\"967cmmVC5bWskCbG2hwAmwcOpeD5eX5D7dugjR2tUxs=\"},\"uri\":{\"S\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-588304f4d5bb438398f6c1776f4f2195.bin\"}}}]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-22T14:19:40.385Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON012\"},\"statusCode\":{\"S\":\"CON012\"},\"statusDateTime\":{\"S\":\"2024-01-22T14:03:09Z\"},\"statusDescription\":{\"S\":\"OK Distinta Elettronica da Recapitista\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[{\"M\":{\"date\":{\"S\":\"2024-01-22T14:41:38Z\"},\"documentType\":{\"S\":\"Distinta Elettronica Sigillata\"},\"id\":{\"S\":\"0\"},\"sha256\":{\"S\":\"n+Sb8gZS/Ajd+BT4LeLoPACUqByAwbYFdCV6rY0QlT8=\"},\"uri\":{\"S\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-16a1e9c34ace4cbd9b2727ebb730cf9c.bin\"}}}]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-22T14:57:57.840Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON011\"},\"statusCode\":{\"S\":\"CON011\"},\"statusDateTime\":{\"S\":\"2024-01-22T14:41:38Z\"},\"statusDescription\":{\"S\":\"Distinta Elettronica Sigillata\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-23T09:20:41.911Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON09A\"},\"statusCode\":{\"S\":\"CON09A\"},\"statusDateTime\":{\"S\":\"2024-01-23T09:06:30Z\"},\"statusDescription\":{\"S\":\"Materialità Pronta\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[{\"M\":{\"date\":{\"S\":\"2024-01-23T14:30:26Z\"},\"documentType\":{\"S\":\"PICKUP Sigillata\"},\"id\":{\"S\":\"0\"},\"sha256\":{\"S\":\"LZnRtbtGN7gwkB4m8FcfmiOp4X4jA4aA12dSOrW662I=\"},\"uri\":{\"S\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-81b49b4564794fc8bda41fd4b86694b8.bin\"}}}]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-23T14:47:49.885Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON016\"},\"statusCode\":{\"S\":\"CON016\"},\"statusDateTime\":{\"S\":\"2024-01-23T14:30:26Z\"},\"statusDescription\":{\"S\":\"PICKUP Sigillata\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[{\"M\":{\"date\":{\"S\":\"2024-01-26T14:21:51Z\"},\"documentType\":{\"S\":\"Accettazione Recapitista\"},\"id\":{\"S\":\"0\"},\"sha256\":{\"S\":\"KVCy7Gg14tvUDm50oeePxh+P3G9OxtevQhT6j+LJbiI=\"},\"uri\":{\"S\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-7c3604f627c64158addcb4531b154b4b.p7m\"}}}]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-26T14:56:11.552Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"NULL\":true},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"CON018\"},\"statusCode\":{\"S\":\"CON018\"},\"statusDateTime\":{\"S\":\"2024-01-26T14:21:51Z\"},\"statusDescription\":{\"S\":\"Accettazione Recapitista\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-01-30T15:52:50.068Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"S\":\"\"},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"RECAG002A\"},\"statusCode\":{\"S\":\"RECAG002A\"},\"statusDateTime\":{\"S\":\"2024-01-29T11:46:42Z\"},\"statusDescription\":{\"S\":\"RECAG002A\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[{\"M\":{\"date\":{\"S\":\"2024-02-15T14:25:14Z\"},\"documentType\":{\"S\":\"23L\"},\"id\":{\"S\":\"0\"},\"sha256\":{\"S\":\"6gxzHRPIQQf0ufu7TZavXqZr2BUBWf9ZzGFlzBbnVD0=\"},\"uri\":{\"S\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-a94a37bbcf06472bae117ab834c2208e.pdf\"}}}]},\"clientRequestTimeStamp\":{\"S\":\"2024-02-16T16:25:17.183Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"S\":\"\"},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"RECAG002B\"},\"statusCode\":{\"S\":\"RECAG002B\"},\"statusDateTime\":{\"S\":\"2024-01-29T11:46:42Z\"},\"statusDescription\":{\"S\":\"RECAG002B\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-02-16T16:57:37.170Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"S\":\"\"},\"productType\":{\"S\":\"23L\"},\"registeredLetterCode\":{\"S\":\"298218259393\"},\"status\":{\"S\":\"REC090\"},\"statusCode\":{\"S\":\"REC090\"},\"statusDateTime\":{\"S\":\"2024-02-16T14:25:14Z\"},\"statusDescription\":{\"S\":\"REC090\"}}}}},{\"M\":{\"digProgrStatus\":{\"NULL\":true},\"paperProgrStatus\":{\"M\":{\"attachments\":{\"L\":[]},\"clientRequestTimeStamp\":{\"S\":\"2024-02-17T01:07:10.157Z\"},\"deliveryFailureCause\":{\"NULL\":true},\"discoveredAddress\":{\"NULL\":true},\"iun\":{\"S\":\"\"},\"productType\":{\"S\":\"890\"},\"registeredLetterCode\":{\"S\":\"398218259394\"},\"status\":{\"S\":\"RECAG002C\"},\"statusCode\":{\"S\":\"RECAG002C\"},\"statusDateTime\":{\"S\":\"2024-01-29T11:46:42Z\"},\"statusDescription\":{\"S\":\"RECAG002C\"}}}}}]},\"paperRequestMetadata\":{\"M\":{\"iun\":{\"NULL\":true},\"printType\":{\"S\":\"BN_FRONTE_RETRO\"},\"productType\":{\"S\":\"890\"},\"requestPaid\":{\"NULL\":true},\"vas\":{\"NULL\":true}}},\"requestHash\":{\"S\":\"2c343be2d009825bbb15b02d21cf911e91f9e09747d1698f28d952de5b6bca83\"},\"requestTimestamp\":{\"S\":\"2023-12-25T19:08:42Z\"},\"statusRequest\":{\"S\":\"RECAG002C\"},\"version\":{\"N\":\"14\"},\"XPagopaExtchCxId\":{\"S\":\"pn-cons-000\"}},\"SizeBytes\":1581},\"eventSource\":\"aws:dynamodb\"}";
        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertTransferCount(DDBKToJson.REL_SUCCESS, 1);

        MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(DDBKToJson.REL_SUCCESS).get(0);
        String resultContent = new String(testRunner.getContentAsByteArray(resultFlowFile));

        String expected = "{\"eventID\":\"aa69e85c-9587-462f-85bb-8af820ae3b93\",\"eventName\":\"INSERT\", \"requestId\":\"pn-cons-000~PREPARE_ANALOG_DOMICILE.IUN_WNUN-RVXQ-UKET-202312-A-1.RECINDEX_0.ATTEMPT_0.PCRETRY_0\",\"clientRequestTimeStamp\":\"2023-12-25T19:08:42.053286098Z\",\"eventsList\":[{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":null,\"clientRequestTimeStamp\":null,\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":null,\"registeredLetterCode\":null,\"status\":\"booked\",\"statusCode\":null,\"statusDateTime\":\"2023-12-25T19:08:42Z\",\"statusDescription\":null}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":null,\"clientRequestTimeStamp\":null,\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":null,\"registeredLetterCode\":null,\"status\":\"sent\",\"statusCode\":\"P000\",\"statusDateTime\":\"2023-12-25T19:10Z\",\"statusDescription\":null}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-01-22T13:44:49.249Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON080\",\"statusCode\":\"CON080\",\"statusDateTime\":\"2024-01-22T13:15:50Z\",\"statusDescription\":\"Stampato ed Imbustato\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-01-22T13:44:49.249Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON010\",\"statusCode\":\"CON010\",\"statusDateTime\":\"2024-01-22T13:15:50Z\",\"statusDescription\":\"Distinta Elettronica inviata a Recapitista\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[{\"date\":\"2024-01-22T14:03:09Z\",\"documentType\":\"OK Distinta Elettronica da Recapitista\",\"id\":\"0\",\"sha256\":\"967cmmVC5bWskCbG2hwAmwcOpeD5eX5D7dugjR2tUxs=\",\"uri\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-588304f4d5bb438398f6c1776f4f2195.bin\"}],\"clientRequestTimeStamp\":\"2024-01-22T14:19:40.385Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON012\",\"statusCode\":\"CON012\",\"statusDateTime\":\"2024-01-22T14:03:09Z\",\"statusDescription\":\"OK Distinta Elettronica da Recapitista\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[{\"date\":\"2024-01-22T14:41:38Z\",\"documentType\":\"Distinta Elettronica Sigillata\",\"id\":\"0\",\"sha256\":\"n+Sb8gZS/Ajd+BT4LeLoPACUqByAwbYFdCV6rY0QlT8=\",\"uri\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-16a1e9c34ace4cbd9b2727ebb730cf9c.bin\"}],\"clientRequestTimeStamp\":\"2024-01-22T14:57:57.840Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON011\",\"statusCode\":\"CON011\",\"statusDateTime\":\"2024-01-22T14:41:38Z\",\"statusDescription\":\"Distinta Elettronica Sigillata\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-01-23T09:20:41.911Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON09A\",\"statusCode\":\"CON09A\",\"statusDateTime\":\"2024-01-23T09:06:30Z\",\"statusDescription\":\"Materialità Pronta\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[{\"date\":\"2024-01-23T14:30:26Z\",\"documentType\":\"PICKUP Sigillata\",\"id\":\"0\",\"sha256\":\"LZnRtbtGN7gwkB4m8FcfmiOp4X4jA4aA12dSOrW662I=\",\"uri\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-81b49b4564794fc8bda41fd4b86694b8.bin\"}],\"clientRequestTimeStamp\":\"2024-01-23T14:47:49.885Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON016\",\"statusCode\":\"CON016\",\"statusDateTime\":\"2024-01-23T14:30:26Z\",\"statusDescription\":\"PICKUP Sigillata\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[{\"date\":\"2024-01-26T14:21:51Z\",\"documentType\":\"Accettazione Recapitista\",\"id\":\"0\",\"sha256\":\"KVCy7Gg14tvUDm50oeePxh+P3G9OxtevQhT6j+LJbiI=\",\"uri\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-7c3604f627c64158addcb4531b154b4b.p7m\"}],\"clientRequestTimeStamp\":\"2024-01-26T14:56:11.552Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":null,\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"CON018\",\"statusCode\":\"CON018\",\"statusDateTime\":\"2024-01-26T14:21:51Z\",\"statusDescription\":\"Accettazione Recapitista\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-01-30T15:52:50.068Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":\"\",\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"RECAG002A\",\"statusCode\":\"RECAG002A\",\"statusDateTime\":\"2024-01-29T11:46:42Z\",\"statusDescription\":\"RECAG002A\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[{\"date\":\"2024-02-15T14:25:14Z\",\"documentType\":\"23L\",\"id\":\"0\",\"sha256\":\"6gxzHRPIQQf0ufu7TZavXqZr2BUBWf9ZzGFlzBbnVD0=\",\"uri\":\"safestorage://PN_EXTERNAL_LEGAL_FACTS-a94a37bbcf06472bae117ab834c2208e.pdf\"}],\"clientRequestTimeStamp\":\"2024-02-16T16:25:17.183Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":\"\",\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"RECAG002B\",\"statusCode\":\"RECAG002B\",\"statusDateTime\":\"2024-01-29T11:46:42Z\",\"statusDescription\":\"RECAG002B\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-02-16T16:57:37.170Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":\"\",\"productType\":\"23L\",\"registeredLetterCode\":\"298218259393\",\"status\":\"REC090\",\"statusCode\":\"REC090\",\"statusDateTime\":\"2024-02-16T14:25:14Z\",\"statusDescription\":\"REC090\"}},{\"digProgrStatus\":null,\"paperProgrStatus\":{\"attachments\":[],\"clientRequestTimeStamp\":\"2024-02-17T01:07:10.157Z\",\"deliveryFailureCause\":null,\"discoveredAddress\":null,\"iun\":\"\",\"productType\":\"890\",\"registeredLetterCode\":\"398218259394\",\"status\":\"RECAG002C\",\"statusCode\":\"RECAG002C\",\"statusDateTime\":\"2024-01-29T11:46:42Z\",\"statusDescription\":\"RECAG002C\"}}],\"paperRequestMetadata\":{\"iun\":null,\"printType\":\"BN_FRONTE_RETRO\",\"productType\":\"890\",\"requestPaid\":null,\"vas\":null},\"requestHash\":\"2c343be2d009825bbb15b02d21cf911e91f9e09747d1698f28d952de5b6bca83\",\"requestTimestamp\":\"2023-12-25T19:08:42Z\",\"statusRequest\":\"RECAG002C\",\"version\":14,\"XPagopaExtchCxId\":\"pn-cons-000\"}\n";
        Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(resultContent));
        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put(DDBKToJson.DDBK_EVENT_ID, "aa69e85c-9587-462f-85bb-8af820ae3b93");
        expectedAttributes.put(DDBKToJson.DDBK_EVENT_NAME, "INSERT");
        expectedAttributes.put(DDBKToJson.DDBK_EVENT_SOURCE, "aws:dynamodb");
        expectedAttributes.put(DDBKToJson.DDBK_AWS_REGION, "eu-south-1");
        expectedAttributes.put(DDBKToJson.DDBK_RECORD_FORMAT, "application/json");
        expectedAttributes.put(DDBKToJson.DDBK_TABLE_NAME, "pn-Notifications");

        Assertions.assertEquals(expectedAttributes, resultFlowFile.getAttributes()
                .entrySet().stream()
                .filter(map -> map.getKey().startsWith("ddbk"))
                .collect(HashMap::new, (m,e)->m.put(e.getKey(),e.getValue()), HashMap::putAll)
                //.collect(Collectors.toMap(Entry::getKey, Entry::getValue))
        );
    }

    @Test
    public void testEmptyInput() throws JsonProcessingException {
        String content = "{}";
        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertTransferCount(DDBKToJson.REL_WARN, 1);

        MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(DDBKToJson.REL_WARN).get(0);
        String resultContent = new String(testRunner.getContentAsByteArray(resultFlowFile));

        String expected = "{}";
        Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(resultContent));
    }

    @Test
    public void testEmptyNewImage() throws JsonProcessingException {
        String content = "{ \"awsRegion\": \"eu-south-1\", \"eventID\": \"aa69e85c-9587-462f-85bb-8af820ae3b93\", \"eventName\": \"INSERT\", \"userIdentity\": null, \"recordFormat\": \"application/json\", \"tableName\": \"pn-Notifications\", \"dynamodb\": { \"ApproximateCreationDateTime\": 1709033076283, \"Keys\": { \"iun\": { \"S\": \"NGTQ-NDUQ-ENGE-202402-N-1\" } }, \"SizeBytes\": 1581 }, \"eventSource\": \"aws:dynamodb\" }";
        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertTransferCount(DDBKToJson.REL_WARN, 1);

        MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(DDBKToJson.REL_WARN).get(0);
        String resultContent = new String(testRunner.getContentAsByteArray(resultFlowFile));
        Assertions.assertEquals(mapper.readTree(content), mapper.readTree(resultContent));
    }

    @Test
    public void testMalformedJSON() {
        String content = "{ \"awsRegion\": \"eu-south-1\", foo, 123}";
        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertTransferCount(DDBKToJson.REL_FAILURE, 1);

        MockFlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(DDBKToJson.REL_FAILURE).get(0);
        String resultContent = new String(testRunner.getContentAsByteArray(resultFlowFile));
        Assertions.assertEquals(content, resultContent);
    }

}
