package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataExtractor;
import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.cs.rfq.decorator.extractors.TotalTradesWithEntityExtractor;
import com.cs.rfq.decorator.extractors.VolumeTradedWithEntityYTDExtractor;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        // Use the TradeDataLoader to load the trade data archives
        TradeDataLoader tradeData = new TradeDataLoader();
        trades = tradeData.loadTrades(this.session, "src/test/resources/trades/trades.json");

        // Take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        // Stream data from the input socket on localhost:9000
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);
        // Convert each incoming line to a Rfq object and call processRfq method with it
        lines.foreachRDD(rdd -> {
            // Split each line and parse it
            rdd.collect().forEach(line -> processRfq(Rfq.fromJson(line)));
        });

        // Start the streaming context
        streamingContext.start();
        // streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        // Create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        // Get metadata from each of the extractors
        // Loop through the extractor list and implement each extractor
        for (RfqMetadataExtractor extractor: extractors) {
            // Use the metadata extractor and add to the metadata map
            for(Map.Entry<RfqMetadataFieldNames, Object> entry: extractor.extractMetaData(rfq, session, trades).entrySet()) {
                metadata.put(entry.getKey(), entry.getValue());
            }
        }

        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
    }
}
