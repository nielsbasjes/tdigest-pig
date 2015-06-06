REGISTER ../target/tdigest-pig-*-udf.jar;

DEFINE TDigestMerge     nl.basjes.pig.stats.tdigest.Merge;
DEFINE TDigestQuantile  nl.basjes.pig.stats.tdigest.Quantile;

InputData = LOAD 'data.txt' AS (value:long);

GroupedData =
    GROUP    InputData ALL;

TDGroup =
    FOREACH  GroupedData
    GENERATE nl.basjes.pig.stats.tdigest.Merge(InputData.value) AS tDigest;

TDStats =
    FOREACH  TDGroup
    GENERATE nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.90) AS Precentile90:double,
             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.75) AS Precentile75:double,
             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.50) AS Precentile50:double,
             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.25) AS Precentile25:double,
             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.10) AS Precentile10:double;

DUMP TDStats;
