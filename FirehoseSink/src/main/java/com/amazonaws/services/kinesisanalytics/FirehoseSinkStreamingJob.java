package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * A Kinesis Data Analytics for Java application that calculates minimum stock price for all stock symbols in a given Kinesis stream over a sliding window and
 * writes output to a Firehose Delivery Stream.
 * <p>
 * To run this application, update the mainClass definition in pom.xml.
 * <p>
 * Note that additional permissions are needed for the IAM role to use Firehose sink:
 *         {
 *             "Sid": "WriteDeliveryStream",
 *             "Effect": "Allow",
 *             "Action": "firehose:*",
 *             "Resource": "arn:aws:firehose:us-west-2:012345678901:deliverystream/ExampleDeliveryStream"
 *         }
 */
public class FirehoseSinkStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(FirehoseSinkStreamingJob.class);

    private static final String PRODUCER_PROPERTIES = "ProducerProperties";
    public static final String STREAM_NAME = "StreamName";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, Properties> applicationProperties = getApplicationProperties();
        Properties producerProperties = applicationProperties.get(PRODUCER_PROPERTIES);
        printProperties(producerProperties);

        env.addSource(new StringSource())
                .name("Strings source")
                .addSink(createSink(producerProperties))
                .name("Firehose sink");

        env.execute("Firehose publisher example");
    }

    private static Map<String, Properties> getApplicationProperties() {
        try {
            return KinesisAnalyticsRuntime.getApplicationProperties();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static FlinkKinesisFirehoseProducer<String> createSink(Properties properties) {
        String streamName = properties.getProperty(STREAM_NAME);
        return new FlinkKinesisFirehoseProducer<>(streamName, new SimpleStringSchema(), properties);
    }

    private static void printProperties(Properties properties) {
        properties.entrySet().stream()
                .map(FirehoseSinkStreamingJob::formatEntry)
                .forEach(LOG::info);
    }

    private static String formatEntry(Map.Entry<Object, Object> entry) {
        return entry.getKey() + " : " + entry.getValue() + System.lineSeparator();
    }
}
