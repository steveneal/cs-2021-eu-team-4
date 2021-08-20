package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {
    private String weekSince;
    private String monthSince;
    private Object ratio;
    private final int buy = 1;
    private final int sell = 2 ;

    public TradeSideBiasExtractor() {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(pastWeekMs).minusMonths(1).getMillis();
        this.weekSince = Long.toString(pastWeekMs);
        this.monthSince = Long.toString(pastMonthMs);
    }

    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        /*
        String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);
         */
        // get the number on sell side for a week
        String weekSellQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND Side = '%s'",
                rfq.getEntityId(),
               // rfq.getIsin(),
                weekSince,
                sell);

        // get the number on buy side for a week
        String weekBuyQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND Side = '%s'",
                rfq.getEntityId(),
               // rfq.getIsin(),
                weekSince,
                buy);

        // get the number on sell side for a month
        String monthSellQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND Side = '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                monthSince,
                sell);

        // get the number on buy side for a month
        String monthBuyQuery = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityID='%s' AND TradeDate >= '%s' AND Side = '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                monthSince,
                buy);

        trades.createOrReplaceTempView("trade");

        // query results
        Dataset<Row> weekSellQueryResults = session.sql(weekSellQuery);
        Dataset<Row> weekBuyQueryResults = session.sql(weekBuyQuery);
        Dataset<Row> monthSellQueryResults = session.sql(monthSellQuery);
        Dataset<Row> monthBuyQueryResults = session.sql(monthBuyQuery);

        // get each first result
        Object weekSellAmount = weekSellQueryResults.first().get(0);
        Object weekBuyAmount = weekBuyQueryResults.first().get(0);
        Object monthSellAmount = monthSellQueryResults.first().get(0);
        Object monthBuyAmount = monthBuyQueryResults.first().get(0);

        // Check if values are null, if they are not then set the ratio else set as a negative value
        if (weekSellAmount != null || weekBuyAmount != null) {
            ratio = (long) weekBuyAmount / (long) weekSellAmount;
        } else if (monthBuyAmount != null || monthSellAmount != null) {
            ratio = (long) monthBuyAmount /(long) monthSellAmount ;
        }
        else {
            ratio = -1L;
        }

        System.out.println(ratio);
        System.out.println(weekBuyAmount);
        System.out.println(weekSellAmount);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradeSideBias, ratio);
        return results;
    };

    protected void setWeekSince(String since) {
        this.weekSince = since;
    }
    protected void setMonthSince(String since) {
        this.monthSince = since;
    }
}
