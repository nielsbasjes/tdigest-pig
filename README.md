T-Digest Pig UDF
======================

This is simply a pig wrapper around [T-Digest](https://github.com/tdunning/t-digest) 
which was created by [Ted Dunning](https://twitter.com/ted_dunning).

Concept of the UDF:
-----
You first feed al of your numbers into the Merge method.
This creates a specially structured Tuple that contains the entire TDigest datastructure.
After this has been aggregated then you can query multiple quantile values from this single datastructure.
So this means the tdigest only needs to be constructed once for a set of numbers.

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
