package com.cs.rfq.decorator.extractors;

//avg price traded by bank over the past week for all instruments included in the RFQ.
//unit and integration testing clearly demonstrates the operation of the required functionality including handling errors.

import com.cs.rfq.decorator.Rfq;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;



public class AvgTradedPriceExtractor implements RfqMetadataExtractor{

    private String since;

    public AvgTradedPriceExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT avg(LastPx) over (partition by entityId) as avgTradePrice from trade  " +
                        "where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object avgTradePrice = sqlQueryResults.first().get(0);
        if (avgTradePrice == null) {
            avgTradePrice = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, avgTradePrice);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }


}
