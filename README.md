piggybank
=========
[![Build Status](https://travis-ci.org/cdgore/piggybank.png?branch=master)](https://travis-ci.org/cdgore/piggybank)

Currently includes Pig UDFs to assist in generating time series data and for reading JSON key-value pairs into a Pig script

**GetIncrementedTimes**

Given a start time, end time, date time format, and time increment, generate date time local bound
tuples of the same format as the inputted global bounds

For example:

```
time_series = FOREACH records GENERATE FLATTEN(GetIncrementedTimes('2013-04-05 12:00:00',
'2013-08-15 16:00:00', 'yyyy-MM-dd HH:mm:ss', 1209600)) AS (local_lower_bound: chararray, 
local_upper_bound: chararray);
DUMP time_series;
```

```
(2013-04-05 12:00:00,2013-04-19 12:00:00)
(2013-04-19 12:00:00,2013-05-03 12:00:00)
(2013-05-03 12:00:00,2013-05-17 12:00:00)
(2013-05-17 12:00:00,2013-05-31 12:00:00)
(2013-05-31 12:00:00,2013-06-14 12:00:00)
(2013-06-14 12:00:00,2013-06-28 12:00:00)
(2013-06-28 12:00:00,2013-07-12 12:00:00)
(2013-07-12 12:00:00,2013-07-26 12:00:00)
(2013-07-26 12:00:00,2013-08-09 12:00:00)
```

Additional options include [-anchorLeft | -anchorRight], -includeTarget
Default is -anchorLeft, which begins the time series at the given lower bound
-anchorRight ends the time series at the given upper bound
-includeTarget generates one additional Tuple on the opposite side of the anchoring bound that
contains the time period for which one would predict

For example:

```
time_series = FOREACH records GENERATE FLATTEN(GetIncrementedTimes('2013-04-05 12:00:00',
'2013-08-15 16:00:00', 'yyyy-MM-dd HH:mm:ss', 1209600, '-includeTarget')) AS 
(local_lower_bound: chararray, local_upper_bound: chararray);
DUMP time_series;
```

```
(2013-03-22 12:00:00,2013-04-05 12:00:00)
(2013-04-05 12:00:00,2013-04-19 12:00:00)
(2013-04-19 12:00:00,2013-05-03 12:00:00)
(2013-05-03 12:00:00,2013-05-17 12:00:00)
(2013-05-17 12:00:00,2013-05-31 12:00:00)
(2013-05-31 12:00:00,2013-06-14 12:00:00)
(2013-06-14 12:00:00,2013-06-28 12:00:00)
(2013-06-28 12:00:00,2013-07-12 12:00:00)
(2013-07-12 12:00:00,2013-07-26 12:00:00)
(2013-07-26 12:00:00,2013-08-09 12:00:00)
```

or anchored on the right side, including the target bounds:

```
time_series = FOREACH records GENERATE FLATTEN(GetIncrementedTimes('2013-04-05 12:00:00',
'2013-08-15 16:00:00', 'yyyy-MM-dd HH:mm:ss', 1209600, 'anchorRight', '-includeTarget')) AS 
(local_lower_bound: chararray, local_upper_bound: chararray);
DUMP time_series;
```

```
(2013-08-15 16:00:00,2013-08-29 16:00:00)
(2013-08-01 16:00:00,2013-08-15 16:00:00)
(2013-07-18 16:00:00,2013-08-01 16:00:00)
(2013-07-04 16:00:00,2013-07-18 16:00:00)
(2013-06-20 16:00:00,2013-07-04 16:00:00)
(2013-06-06 16:00:00,2013-06-20 16:00:00)
(2013-05-23 16:00:00,2013-06-06 16:00:00)
(2013-05-09 16:00:00,2013-05-23 16:00:00)
(2013-04-25 16:00:00,2013-05-09 16:00:00)
(2013-04-11 16:00:00,2013-04-25 16:00:00)
```


**JsonKVPairsToBag**

Accepts a JSON chararray containing {key: value} pairs and returns a DataBag containing
Tuples of the key value pairs. For example:

INPUT:
```
{"email_open" : "0.014909921373024116", "email_click" : "0.013381788642734023", "click" : "0.032996975306163016", "purchase" : "7.7345610760846455", "shopping_cart" : "2.3260353693494427", "view" : "0.7275636800328681"}
```

OUTPUT:
```
(email_open,0.014909921373024116)
(email_click,0.013381788642734023)
(click,0.032996975306163016)
(purchase,7.7345610760846455)
(shopping_cart,2.3260353693494427)
(view,0.7275636800328681)
```