package com.cs.rfq.decorator.extractors;
import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;


public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {
    private String since;

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        this.since = Long.toString(pastMonthMs);

        // SQL query for all trades in the past month
        String query = String.format("SELECT SUM(LastQty) over (partition by entityId) as Liquidity from trade  " +
                        "where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'"
                , rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> tradesPastMonth = session.sql(query + "'" + new java.sql.Date(pastMonthMs) + "'");

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(liqudityTradedForPastMonth, tradesPastMonth.first().get(0) == null ? 0L : tradesPastMonth.first().get(0));
        return results;
    }
    protected void setSince(String since) {
        this.since = since;
    }

}
