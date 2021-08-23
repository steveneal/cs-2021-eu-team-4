package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    private VolumeTradedForSecurityExtractor extractor;
    private Map<RfqMetadataFieldNames, Object> meta;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeForPastWeek() {
        Object result = meta.get(RfqMetadataFieldNames.volumeTradedForSecurityPastWeek);
        assertEquals(10L, result);
    }

    @Test
    public void checkVolumeForPastMonth() {
        Object result = meta.get(RfqMetadataFieldNames.volumeTradedForSecurityPastMonth);
        assertEquals(20L, result);
    }

    @Test
    public void checkVolumeForPastYear() {
        Object result = meta.get(RfqMetadataFieldNames.volumeTradedForSecurityPastYear);
        assertEquals(30L, result);
    }
}

