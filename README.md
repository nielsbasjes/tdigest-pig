T-Digest Pig UDF
======================

This is a Pig wrapper around [t-digest](https://github.com/tdunning/t-digest).

Build:
------

    git clone https://github.com/nielsbasjes/tdigest-pig
    
Now build and install the java version:

    cd tdigest-pig
    mvn install 

NOTE: You may have to skip the testing in case there is a problem 

    mvn install -DskipTests=true

Usage:
--------
```pig
REGISTER tdigest-*-pig.jar

@@@ TODO: WRITE EXAMPLE @@@


```

Author:
-------

  * Niels Basjes [@nielsbasjes](https://twitter.com/nielsbasjes)

  This is a trivial interface on top of the T-Digest created by Ted Dunning.
