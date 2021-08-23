package com.cs.rfq.decorator.extractors;
import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;


public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {
    private String since;

    public InstrumentLiquidityExtractor() {
        // Get one month
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        this.since = Long.toString(pastMonthMs);
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {


        // SQL query for all trades in the past month
        String query = String.format("SELECT SUM(LastQty) as Liquidity from trade  WHERE SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object liquidity = sqlQueryResults.first().get(0);
        if (liquidity == null) {
            liquidity = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.liquidity, liquidity);
        System.out.println(results);
        return results;
    }
    protected void setSince(String since) {
        this.since = since;
    }

}
