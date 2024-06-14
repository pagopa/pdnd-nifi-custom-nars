package it.pagopa.pdnd.nifi.processors.ddbtojson.model;

import org.apache.nifi.processor.Relationship;

public class DynamoConvertedOutputJson {
    private String outputJson;
    private Relationship failRelationship;

    private DynamoConvertedOutputJson(Builder builder) {
        this.outputJson = builder.outputJson;
        this.failRelationship = builder.failRelationship;
    }

    public static class Builder {
        private String outputJson;
        private Relationship failRelationship;

        public Builder withOutputJson(String outputJson) {
            this.outputJson = outputJson;
            return this;
        }

        public Builder withFailRelationship(Relationship failRelationship) {
            this.failRelationship = failRelationship;
            return this;
        }

        public DynamoConvertedOutputJson build() {
            return new DynamoConvertedOutputJson(this);
        }
    }

    public String getOutputJson() {
        return outputJson;
    }

    public Relationship getFailRelationship() {
        return failRelationship;
    }
}
