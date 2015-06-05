REGISTER ../target/tdigest-pig-*-udf.jar;

DEFINE TDigestMerge     nl.basjes.pig.tdigest.Merge;
DEFINE TDigestQuantile  nl.basjes.pig.tdigest.Quantile;

Data = LOAD 'data.txt' AS (value:long);

GroupedData = GROUP Data ALL;

TDGroup =
    FOREACH GroupedData
    GENERATE TDigestMerge(Data.value) AS tDigest:bytearray;

TDValues =
    FOREACH     TDGroup
    GENERATE    TDigestQuantile(tDigest,0.9) AS Precentile9:double,
                TDigestQuantile(tDigest,0.99) AS Precentile99:double,
                TDigestQuantile(tDigest,0.999) AS Precentile999:double,
                TDigestQuantile(tDigest,0.5) AS Precentile5:double;

DUMP TDValues;
