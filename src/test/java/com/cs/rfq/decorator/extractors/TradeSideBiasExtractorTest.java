package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {
    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkBiasForMonthWhenAllTradesMatch() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setMonthSince("2018-06-09");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBias);

        assertEquals(1L, result);
    }

    @Test
    public void checkBiasForMonthWhenNoTradesMatch() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setMonthSince("2019-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBias);

        assertEquals(-1L, result);
    }

    @Test
    public void checkBiasForWeekWhenAllTradesMatch() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setWeekSince("2018-06-09");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBias);

        assertEquals(1L, result);
    }

    @Test
    public void checkBiasForWeekWhenNoTradesMatch() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setWeekSince("2019-06-09");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBias);

        assertEquals(-1L, result);
    }
}
