package com.example.kafkastreamstudy.athena.aggregator;

public class AnalyzerResult {
    private String contentKey;
    private String snapshotId;
    private Integer analyzerCode;
    private String analyzerResultId;
    private String featureKey;
    private String featureValue;
    private FeatureType featureType;
    private Disposition disposition;
    private Boolean visualizationFlag;

    public enum FeatureType {
        BLACK, WHITE, FEATURE, UNKNOWN
    }

    public enum Disposition {
        SPAM
    }
}
