T-Digest Pig UDF
======================

This is simply a pig wrapper around [T-Digest](https://github.com/tdunning/t-digest) 
which was created by [Ted Dunning](https://twitter.com/ted_dunning).

Build:
------

    git clone https://github.com/nielsbasjes/tdigest-pig
    
Now build and install the java version:

    cd tdigest-pig
    mvn install 

Usage:
--------
```
REGISTER ../target/tdigest-pig-*-udf.jar;

DEFINE TDigestMerge     nl.basjes.pig.stats.tdigest.Merge;
DEFINE TDigestQuantile  nl.basjes.pig.stats.tdigest.Quantile;

InputData = 
    LOAD 'data.txt' 
    AS  (value:long);

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
```

Author:
-------

  Niels Basjes [@nielsbasjes](https://twitter.com/nielsbasjes)
