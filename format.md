# Archive Access and Formats

Archive data is stored in Google's storage cloud organized by date at the prefix: 

```
https://storage.googleapis.com/evekit_md/YYYY/MM/DD
```

Market history and order book data are stored separately.  Market history uses the prefix "market" in all files.
Order books use the prefix "interval" in all files.  Currently, order books are snapped at five minute intervals
which corresponds to the minimum cache time for the associated CREST endpoints.  The interval snap time
is indicated by the "5" suffix in the interval file names.  Our generation tools are capable of sampling data
at other intervals, but currently we only provide data with five minute intervals.

Three files are stored for each data type:

|File Name|Description|
|---------|-----------|
|**market_YYYYMMDD.tgz** or **interval_YYYYMMDD_5.tgz**|Downloadable archive of daily market history or order books.  Files include data for all regions on the given day.|
|**market_YYYYMMDD.bulk** or **interval_YYYYMMDD_5.bulk**|Daily data in "bulk" form, designed to be partially retrieved via an HTTP "range" request.  This format is intended to be used by online tools which only need a subset of data, perhaps to drive web-based tools.|
|**market_YYYYMMDD.index.gz** or **interval_YYYYMMDD_5.index.gz**|Index file which provides offsets into the "bulk" files based on EVE market type.  This file is read first to determine the appropriate byte offset for a "range" request to the bulk files.|

## Download File Format
      
The first file type in the table above is designed to be downloaded for offline use.  These files are tar'd and gzip'd archives of
market data organized by EVE market type:

|File Name|Entry Name|Description|
|---------|----------|-----------|
|market_YYYYMMDD.tgz|market_TYPE_YYYYMMDD.history.gz|Market history for the given type and day across all regions.|
|interval_YYYYMMDD_5.tgz|interval_TYPE_YYYYMMDD_5.book.gz|All order books for the given type and day across all regions.|

All files store data in CSV form, but gzip'd to save space.  Market history files are typically less than 2 MB in archive form.
Order book (interval) files are substantially larger, typically around 200 MB in archive form.

Market history entries contain one line per region.  Each line has the format:

|Field|Description|
|-----|-----------|
|TYPE|EVE market type ID|
|REGION|EVE market region ID|
|ORDER COUNT|Number of market orders for this type in this region on this day|
|LOW PRICE|Low trade price for this type in this region on this day|
|HIGH PRICE|High trade price for this type in this region on this day|
|AVERAGE PRICE|Average trade price for this type in this region on this day|
|VOLUME|Daily volume for this type in this region|
|DATE|History date in milliseconds UTC (since the epoch)|

Order book interval files have a more complicated format:

```
TYPE
SNAPSHOTS_PER_REGION
FIRST_REGION_ID
FIRST_REGION_FIRST_SNAPSHOT_TIME
FIRST_REGION_FIRST_SNAPSHOT_BUY_ORDER_COUNT
FIRST_REGION_FIRST_SNAPSHOT_SELL_ORDER_COUNT
FIRST_REGION_FIRST_SNAPSHOT_BUY_ORDER
...
FIRST_REGION_FIRST_SNAPSHOT_SELL_ORDER
...
FIRST_REGION_SECOND_SNAPSHOT_TIME
...
SECOND_REGION_ID
...
```

The "SNAPSHOTS_PER_REGION" field indicates the number of book snapshots stored for each type and region.
With the current default of 5 minute snapshots, this value will always be 288.  Therefore, snapshot times 
(e.g. FIRST_REGION_FIRST_SNAPSHOT_TIME) will also be fixed, starting at 00:00 UTC and incrementing by 
5 minute intervals to 23:55 UTC.

All fields are always present except for individual orders which may be missing if there are no buy
or sell orders for a given region at a given time.

Each buy or sell order line is in CSV format with the following fields:

|Field|Description|
|-----|-----------|
|ORDER ID|Unique market order ID.|
|BUY|"true" if this order represents a buy, "false" otherwise.|
|ISSUED|Order issue date in milliseconds UTC (since the epoch).|
|PRICE|Order price.|
|VOLUME ENTERED|Volume entered when order was created.|
|MIN VOLUME|Minimum volume required for each order fill.|
|VOLUME|Current remaining volume to be filled in the order.|
|ORDER RANGE|Order range string.  One of "station", "solarsystem", "region" or a number representing the number of jobs allowed from the station where the order was entered.|
|LOCATION ID|Location ID of station where order was entered.|
|DURATION|Order duration in days.|
      
## Reading Bulk Data

Bulk data is formatted to allow easy sampling of individual market history or order book data.  Reading this data involves a two step process:

* Fetch the index file and find the offset of the desired type.
* Use a "range" HTTP request to read the appropriate data from the bulk file.

Index files are compressed lists of pairs giving each type and the offset into the bulk file where data for that type is stored.  For example:

```
$ curl https://storage.googleapis.com/evekit_md/2016/06/24/market_20160624.index.gz | zcat
market_18_20160624.history.gz 0
market_19_20160624.history.gz 1033
market_20_20160624.history.gz 1683
market_21_20160624.history.gz 3374
market_22_20160624.history.gz 4089
market_34_20160624.history.gz 4804
market_35_20160624.history.gz 8432
market_36_20160624.history.gz 11935
market_37_20160624.history.gz 15258
...
```

To read market history for type 34 (Tritanium), for example, we need to read data starting at offset 4804 and ending at offset
8431 (the offset for type 35 minus one).  This can be achieved with an HTTP "range" request on the bulk file.  For example:

```
$ curl -H "range: bytes=4804-8431" https://storage.googleapis.com/evekit_md/2016/06/24/market_20160624.bulk | zcat
34,10000025,16,5.51,5.51,5.51,19028609,1466726400000
34,10000027,3,4.90,4.90,4.90,47864147,1466726400000
34,10000028,19,5.04,5.04,5.04,4697021,1466726400000
34,10000029,29,6.00,6.10,6.00,30600727,1466726400000
34,10000030,352,5.16,5.81,5.41,482468387,1466726400000
34,10000016,406,5.49,5.90,5.60,289408396,1466726400000
34,10000018,1,3.03,3.03,3.03,226468,1466726400000
34,10000020,198,4.95,5.21,5.20,379550710,1466726400000
34,10000021,6,6.00,6.00,6.00,14742118,1466726400000
34,10000022,5,2.75,2.75,2.75,213126126,1466726400000
...
```

Interval (order book) data is accessed in a similar fashion.

### NOTE: Market History Bulk Files with Zlib/Node.js

Bulk files are created by concatenating gzip'd files representing data for specific types.
In the case of market history, each row is actually a single gzip'd file.  So when reading market history
for a given type, you are actually reading the concatenation of several individual gzip'd files.
For standard gzip libraries this isn't a problem.  As shown in the example output above, zcat is more
than happy to read through several concatenated gzip files.  But in Node.js with the standard ZLib
library this isn't the case.  Zlib will stop reading after at the end of the first gzip'd buffer,
regardless of whether the remainder of the buffer contains other gzip'd content.  If you need to work
with the bulk files from Node.js, then you'll either need to use a different Zlib, or detect splits
in the bulk file by looking for the gzip header magic bytes.  The latter approach is used in the 
[EveKit MarketData Server](https://github.com/OrbitalEnterprises/evekit-marketdata-server).
Take a look at [evekit_market.js](https://github.com/OrbitalEnterprises/evekit-marketdata-server/blob/master/api/helpers/evekit_market.js) for example code.
It is **not** necessary to do this with interval (order book) data.  Data for each type
in the interval files consists of a single gzip'd file.
