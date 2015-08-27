package com.elasticm2m.frameworks.aws.kinesis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.nio.ByteBuffer;
import java.util.Map;

public class KinesisWriter extends ElasticBaseRichBolt {

    private AWSCredentialsProvider credentialsProvider;
    private String streamName;
    private AmazonKinesis kinesis;
    private boolean logTuple = false;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String body = tuple.getString(1);
            // use groupBy tuple value as partitionKey
            String partitionKey = tuple.size() > 3 ? tuple.getString(3) : "";

            PutRecordRequest request = new PutRecordRequest()
                    .withStreamName(streamName)
                    .withPartitionKey(partitionKey)
                    .withData(ByteBuffer.wrap(body.getBytes()));
            kinesis.putRecord(request);

            if (logTuple) {
                logger.info(body);
            } else {
                logger.debug("Published record to kinesis");
            }

            collector.ack(tuple);
        } catch (Throwable ex) {
            logger.error("Error writing the entity to Kinesis:", ex);
            collector.fail(tuple);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("Kinesis Writer: Stream Name = " + streamName);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        if (credentialsProvider == null) {
            kinesis = new AmazonKinesisAsyncClient();
        } else {
            kinesis = new AmazonKinesisClient(credentialsProvider);
        }
    }

    @Inject
    public void setLogTupple(@Named("log-tuple") boolean logTuple) {
        this.logTuple = logTuple;
    }

    @Inject
    public void setStreamName(@Named("kinesis-stream-name") String streamName) {
        this.streamName = streamName;
    }
}
