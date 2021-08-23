package com.cs.rfq.decorator.extractors;
import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class AverageTradedPriceExtractor implements RfqMetadataExtractor {

    private String since;

    public AverageTradedPriceExtractor() {
        // Get one week
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        this.since = Long.toString(pastWeekMs);
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        // SQL query for all trades in the past week
        String query = String.format("SELECT AVG(LastPx) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object averageTrade = sqlQueryResults.first().get(0);
        if (averageTrade == null) {
            averageTrade = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageTradedPrice, averageTrade);
        System.out.println(results);
        return results;
    };

    protected void setSince(String since) {
        this.since = since;
    }
}
