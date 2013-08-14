package com.cdgore;

import java.io.IOException;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.hadoop.io.Text;

/** 
 * Given a start time, end time, date time format, and time increment, generate date time local bound
 * tuples of the same format as the inputted global bounds
 * 
 * For example:
 *
 * time_series = FOREACH records GENERATE FLATTEN(GetIncrementedTimes('2013-04-05 12:00:00', '2013-08-13 12:00:00', 'yyyy-MM-dd HH:mm:ss', 604800)) AS (local_lower_bound: chararray, local_upper_bound: chararray);
 * DUMP time_series;
 * 
 * (2013-04-05 12:00:00,2013-04-12 12:00:00)
 * (2013-04-12 12:00:00,2013-04-19 12:00:00)
 * (2013-04-19 12:00:00,2013-04-26 12:00:00)
 * (2013-04-26 12:00:00,2013-05-03 12:00:00)
 * (2013-05-03 12:00:00,2013-05-10 12:00:00)
 * (2013-05-10 12:00:00,2013-05-17 12:00:00)
 * (2013-05-17 12:00:00,2013-05-24 12:00:00)
 * (2013-05-24 12:00:00,2013-05-31 12:00:00)
 * (2013-05-31 12:00:00,2013-06-07 12:00:00)
 * (2013-06-07 12:00:00,2013-06-14 12:00:00)
 * (2013-06-14 12:00:00,2013-06-21 12:00:00)
 * (2013-06-21 12:00:00,2013-06-28 12:00:00)
 * (2013-06-28 12:00:00,2013-07-05 12:00:00)
 * (2013-07-05 12:00:00,2013-07-12 12:00:00)
 * (2013-07-12 12:00:00,2013-07-19 12:00:00)
 * (2013-07-19 12:00:00,2013-07-26 12:00:00)
 * (2013-07-26 12:00:00,2013-08-02 12:00:00)
 * (2013-08-02 12:00:00,2013-08-09 12:00:00)
 * 
 **/

public class GetIncrementedTimes extends EvalFunc<DataBag> {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  
  @Override
  public DataBag exec(final Tuple input) throws IOException {
    if (input == null)
      throw new IOException("input is null");
    
    final DataBag resultBag = this.BAG_FACTORY.newDefaultBag();
    final Object firstDateTime = input.get(0);
    final Object lastDateTime = input.get(1);
    final Object dateTimeFormat = input.get(2);
    final Object timeInc = input.get(3);
    
    if (!(firstDateTime instanceof String) || !(lastDateTime instanceof String) || !(dateTimeFormat instanceof String) || !(timeInc instanceof Integer))
      throw new IOException("Expected all inputs to be (chararray, chararray, chararray, int) but instead received (" +
        firstDateTime.getClass().getName() + ", " + lastDateTime.getClass().getName() + " , " + 
          dateTimeFormat.getClass().getName() + ", " + timeInc.getClass().getName() + ")");
    
    final String firstDateTimeString = (String) firstDateTime;
    final String lastDateTimeString = (String) lastDateTime;
    final String formatString = (String) dateTimeFormat;
    final int timeIncSeconds = (Integer) timeInc;
    // final String formatString = "yyyy-MM-dd";
    // final String formatString = "yyMMddHHmm";
    
    SimpleDateFormat sdf = new SimpleDateFormat(formatString);
    
    // Immutable calendars for global bounds on time series
    final Calendar globalTimeLowerBound = Calendar.getInstance();
    final Calendar globalTimeUpperBound = Calendar.getInstance();
    try {
      globalTimeLowerBound.setTime(sdf.parse(firstDateTimeString));
      globalTimeUpperBound.setTime(sdf.parse(lastDateTimeString));
    } catch (ParseException e) {
      throw new IOException(e);
    }
    
    // For each time interval, generate an upper and lower bound date-time formatted the same way
    // as the input global upper and lower bounds
    // TODO: Compare localUpperBound with globalUpperBound rather than localLowerBound
    for (Calendar localTimeLowerBound = (Calendar)globalTimeLowerBound.clone(); localTimeLowerBound.compareTo(globalTimeUpperBound) <= 0; localTimeLowerBound.add(Calendar.SECOND, timeIncSeconds)) {
      try {
        // Clone the lower bound dateTime for this tuple and add the incremental value to it
        // to generate the upper bound
        final Calendar localTimeUpperBound = (Calendar)localTimeLowerBound.clone();
        localTimeUpperBound.add(Calendar.SECOND, timeIncSeconds);
        
        final Tuple returnTuple = TUPLE_FACTORY.newTuple(2);
        returnTuple.set(0, sdf.format(localTimeLowerBound.getTime()));
        returnTuple.set(1, sdf.format(localTimeUpperBound.getTime()));
        resultBag.add(returnTuple);
      } catch (Exception e) {
        System.err.println("ERROR: " + e);
      }
    }
    return resultBag;
  }
}
