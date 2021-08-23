package com.cs.rfq.decorator.extractors;

/**
 * Enumeration of all metadata that will be published by this component
 */
public enum RfqMetadataFieldNames {

    // For TotalTradesWithEntityExtractor


    tradesWithEntityToday,

    tradesWithEntityPastWeek,
    tradesWithEntityPastMonth,
    tradesWithEntityPastYear,
    liquidity,

    // VolumeTradedForInstrumentExtractor
    volumeTradedYearToDate,


    // AverageTradedPriceExtractor
    averageTradedPrice,

    // VolumeTradedForInstrumentExtractor
    volumeTradedForSecurityPastWeek,
    volumeTradedForSecurityPastMonth,
    volumeTradedForSecurityPastYear,

    tradeSideBias,

    liqudityTradedForPastMonth
}
