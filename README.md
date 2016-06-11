# EveKit MarketData Downloader and Generator

This module provides tools for regularly downloading EVE Online marketdata, and for converting market data snapshots into files useful for market analysis.  These tools are part of the [EveKit](https://evekit.orbital.enterprises) collection for EVE Online 3rd party developers.

## EVE Online Market Data

EVE Online market data is exposed through [CREST](http://eveonline-third-party-documentation.readthedocs.io/en/latest/crest/index.html).  There are two main types of data exposed:

 * Order Book: a view of the current bid and ask orders for a particular market type in a particular region.
 * Market History: the daily volume, order count, average, low and high prices for a market type in a particular region.
 
The order book updates frequently, with the current cache timer set to 5 minutes.  Market history updates much less frequently (as you might expect), with a cache timer of around eight hours.  As the order book updates very frequently, book data can be large, particularly in very busy regions (think: Forge).  Market history essentially only updates once a day and is very small.

We won't discuss the specific market data endpoints here.  See the online documentation at the link above for more details.

This module provides three tools for handling marketdata:

 * MarketDownloader: this tool retrieves book data and market history in raw form at a configurable interval.
 * GenerateBooks: this tool consolidates raw book data into one file per type and date.
 * GenerateMarketHistory: this tool consolidates raw market history data into one file per type and date.
 
We describe each of these tools below.

## MarketDownloader

The MarketDownloader retrieves and stores snapshots of both order books and market history at a configurable interval.  The downloader is driven by a database table listing regions and market types we wish to download.  The database is initially populated by downloading all market types and regions from CREST.  If you leave the downloader running continuously, it will update the region and type tables automatically once a day (but will respect any regions or types you have chosen not to download).  We discuss this more in the configuration section below.

During normal operation, the downloader will start a configurable number of threads assigned to either download order books or market history.  The download rate is configurable.  Threads will wait as necessary until it is time to update a book or market history.  With a reasonably powerful machine (we're using a Digital Ocean droplet with 2 cores, 4 GB of memory and 60 GB disk) it is possible to keep up with the minimal cache timers for all regions and market types.

### Order Book Download

For order books, we use the "region" marketdata endpoint which downloads all orders for a single region.  These orders are stored in a gzip'd snapshot file where the first line is the total number of orders stored in the snapshot and the remaining lines are the individual orders (one order per line).  An order line is comma separated text with the following fields:

 * region ID
 * type ID
 * order ID (unique)
 * "true" if the order is a buy, "false" otherwise
 * issue date in milliseconds UTC (since the epoch)
 * price
 * volume entered when the order was created
 * minimum order volume for matching orders
 * current order volume
 * order range (text)
 * order location ID
 * order duration (days) 

At the minimum sensible update interval (currently 5 minutes), each region will have 288 files per day.  There are currently 100 regions in EVE Online, so each day will product 28800 files at maximum resolution.

### Market History Download

For market history, we use the market history endpoint which provides about 13 months of history for a single type and region pair.  The complete snapshot for a type-region pair is stored in a single gzip'd file.  The first line of this file is the total number of entries.  The remaining lines record market history (one history entry per line).  A history line is comma separated text with the following fields:

 * type ID
 * region ID
 * order count
 * low price
 * high price
 * average price
 * total volume
 * date in milliseconds UTC (since the epoch)

History data changes daily, therefore it is only necessary to download data once a day.  With 100 EVE Online regions, and about 13000 market types, a full day of market history will create 1300000 files.

## GenerateBooks

The book generator consolidates raw order book snapshots for a given date into a single file per type.  Consolidation happens by building the complete order book for a given type, then sampling the book during the given day at a configurable interval.  For example, if 30 minute intervals are requested, then the output file will report the complete order book in each region as it appeared every 30 minutes.

The consolidated output file is named as follows:

```
<year>/<month>/<day>/<prefix>_<typeID>_<YYYYMMDD>_<interval_in_minutes>.book.gz
```

The data within the file is organized as follows:

```
typeID
snapshots per region
first regionID
first region first snapshot time in milliseconds UTC
first region first snapshot Number of Buy orders
first region first snapshot Number of Sell orders
first region first snapshot Buy orders, one per line, highest price first
first region first snapshot Sell orders, one per line, lowest price first
first region second snapshot...
second regionID
...
```

Each order is a comma separated line with the following format:

 * order ID
 * buy - "true" if the order is a buy, "false" otherwise
 * issued date in milliseconds UTC
 * price
 * volume entered
 * minimum volume
 * current volume
 * order range
 * location ID
 * duration

## GenerateMarketHistory

The market history generator consolidates raw market history snapshots for a given date into a single file per type containing market history data for all regions for the given date.  The consolidated output file is named as follows:

```
<year>/<month>/<day>/<prefix>_<typeID>_<YYYYMMDD>.history.gz
```

Each line in the output file is comma separated with the following fields:

 * type ID
 * region ID
 * order count
 * low price
 * high price
 * average price
 * volume
 * date in milliseconds UTC

# Setup

After cloning this module, you need to setup a database instance using your favorite JDBC and Hibernate compatible SQL database.  You can create the necessary tables using the following schema:

```
CREATE TABLE ekmd_instrument (
   typeID int(11) NOT NULL,
   active bit(1) NOT NULL,
   lastUpdate bigint(20) NOT NULL,
   scheduleTime bigint(20) NOT NULL,
   scheduled bit(1) NOT NULL,
   PRIMARY KEY (typeID),
   KEY typeIndex (typeID),
   KEY activeIndex (active),
   KEY lastUpdateIndex (lastUpdate),
   KEY scheduledIndex (scheduled),
   KEY scheduleTimeIndex (scheduleTime)
 );

CREATE TABLE ekmd_region (
   regionID int(11) NOT NULL,
   active bit(1) NOT NULL,
   lastUpdate bigint(20) NOT NULL,
   scheduleTime bigint(20) NOT NULL,
   scheduled bit(1) NOT NULL,
   PRIMARY KEY (regionID),
   KEY regionIndex (regionID),
   KEY activeIndex (active),
   KEY lastUpdateIndex (lastUpdate),
   KEY scheduledIndex (scheduled),
   KEY scheduleTimeIndex (scheduleTime)
 ); 
```

These tables will be populated automatically the first time the MarketDownloader is started.

We normally build the module using [Maven](http://maven.apache.org).  The pom.xml file defines several properties which can be overridden in your settings.xml file.  You may need to modify the following properties:

| Parameter | Meaning |
|-----------|---------|
|enterprises.orbital.evekit.marketdata.db.driver|Hibernate JDBC driver class name|
|enterprises.orbital.evekit.marketdata.db.dialect|Hibernate dialect class name|
|enterprises.orbital.evekit.marketdata.db.url|Hibernate JDBC connection URL|
|enterprises.orbital.evekit.marketdata.db.user|Hibernate JDBC connection user name|
|enterprises.orbital.evekit.marketdata.db.password|Hibernate JDBC connection password|
|enterprises.orbital.evekit.marketdata.historyDir|Directory where raw history snapshots will be stored|
|enterprises.orbital.evekit.marketdata.regionDir|Directory where raw region order snapshots will be stored|
|enterprises.orbital.evekit.marketdata.historyThreads|Number of threads to use for market history retrieval|
|enterprises.orbital.evekit.marketdata.regionThreads|Number of threads to use for region order retrieval|
|enterprises.orbital.evekit.marketdata.minRegionSchedInterval|Time (milliseconds) between order book retrievals for each region|
|enterprises.orbital.evekit.marketdata.minHistorySchedInterval|Time (milliseconds) between market history retrievals for each type| 
|enterprises.orbital.evekit.marketdata.booksInParallel|The number of types to assign to each book generation thread (see below)|

# Running the Tools

The MarketDownloader tool has no command line arguments (all configuration is determined from the properties file) and can be invoked from Maven as follows:

```
mvn exec:java -Dexec.mainClass="enterprises.orbital.evekit.marketdata.downloader.MarketDownloader"
```

You can modify the `logging.properties` file as needed to view downloader progress.

The GenerateBooks tool has the following usage:

```
GenerateBooks [-h] [-d <dir>] [-i intervalSizeInMin] [-w YYYY-MM-DD] [-p prefix] [-t booksPerCycle] [-m typeid]
```

where (all arguments are optional):

 * -h prints usage and exits.
 * -d gives the directory where consolidated books will be written.  The default is the current directory.
 * -i gives the sampling interval in minutes for the output files.  The default is 60.
 * -w gives the date for which consolidated books will be generated.  The default is the current date.
 * -p gives the prefix that will be added to each consolidated output file.  The default is "interval".
 * -t gives the number of threads to use for book file generation.  Each thread will build and output books for `booksInParallel` types.  The default is 1.
 * -m if specified (may be specified multiple times), names an explicit type ID which should be output.  If no type IDs are specified, then consolidated books are output for all types.

You can invoke this tool from Maven as follows:

```
mvn exec:java -Dexec.mainClass="enterprises.orbital.evekit.marketdata.generator.GenerateBooks" -Dexec.args="...args..."
```

Finally, the GenerateMarketHistory tool has the following usage:

```
GenerateMarketHistory [-h] [-d <dir>] [-w YYYY-MM-DD] [-p prefix] [-t threads] [-m typeid] [-l YYYY-MM-DD]
```

where (all arguments are optional):

 * -h prints usage and exits.
 * -d gives the directory where consolidated market history will be written.  The default is the current directory.
 * -w gives the date of the market history raw snapshot to use.  The default is the current date.
 * -p gives the prefix that will be added to each consolidated output file.  The default is "market".
 * -t gives the number of threads to use for market history file generation.  The default is 1.
 * -m if specified (may be specified multiple times), names an explicit type ID which should be output.  If no type IDs are specified, then market history is output for all types.
 * -l gives a "limit date" before which no market history will be output.  The default is about two years in the past (well beyond what the current CREST endpoint provides).
 
You can invoke this tool from Maven using the instructions above (substitute GenerateMarketHistory for GenerateBooks). 

## Getting Help

The best place to get help is on the [Orbital Forum](https://groups.google.com/forum/#!forum/orbital-enterprises).
