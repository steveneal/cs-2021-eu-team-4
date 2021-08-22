package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

/**
 * The VolumeTradedForSecurityExtractor program implements a method that returns information of the number of trades
 * made by an entity, for a security over the past week, month and year.
 */

public class VolumeTradedForSecurityExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        String query = String.format("SELECT SUM(LastQty) FROM trade WHERE EntityId = '%s' AND SecurityID = '%s' AND TradeDate >= ",
                rfq.getEntityId(),
                rfq.getIsin());

        trades.createOrReplaceTempView("trade");
        Dataset<Row> tradesPastWeek = session.sql(query + "'" + new java.sql.Date(pastWeekMs) + "'");
        Dataset<Row> tradesPastMonth = session.sql(query + "'" + new java.sql.Date(pastMonthMs) + "'");
        Dataset<Row> tradesPastYear = session.sql(query + "'" + new java.sql.Date(pastYearMs) + "'");

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeTradedForSecurityPastWeek, tradesPastWeek.first().get(0) == null ? 0L : tradesPastWeek.first().get(0));
        results.put(volumeTradedForSecurityPastMonth, tradesPastMonth.first().get(0) == null ? 0L : tradesPastMonth.first().get(0));
        results.put(volumeTradedForSecurityPastYear, tradesPastYear.first().get(0) == null ? 0L : tradesPastYear.first().get(0));
        return results;
    }
}
