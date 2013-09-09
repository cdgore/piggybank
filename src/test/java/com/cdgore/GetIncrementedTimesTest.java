package com.cdgore;

import java.io.IOException;

import org.junit.Test;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

public class GetIncrementedTimesTest {
  @Test
  public void testAnchorLeft() throws IOException, ParseException {
    final String[] script = {
      "times = LOAD 'input' USING PigStorage() AS (time: chararray);",
      "times_group = GROUP times ALL;",
      "times_bounds = FOREACH times_group GENERATE MIN(times.time) AS min_time, MAX(times.time) AS max_time;",
      "time_series_bounds = FOREACH times_bounds GENERATE FLATTEN(rsciudfs.GetIncrementedTimes(min_time, max_time, 'yyyy-MM-dd HH:mm:ss', 1209600, '-anchorLeft')) AS (time_step_lower_bound: chararray, time_step_upper_bound: chararray);",
      "STORE time_series_bounds INTO 'output';"
    };
    
    final String[] input = {
      "2013-04-05 12:00:00",
      "2013-08-15 16:00:00"
    };
    
    final String[] output = {
      "(2013-04-05 12:00:00,2013-04-19 12:00:00)",
      "(2013-04-19 12:00:00,2013-05-03 12:00:00)",
      "(2013-05-03 12:00:00,2013-05-17 12:00:00)",
      "(2013-05-17 12:00:00,2013-05-31 12:00:00)",
      "(2013-05-31 12:00:00,2013-06-14 12:00:00)",
      "(2013-06-14 12:00:00,2013-06-28 12:00:00)",
      "(2013-06-28 12:00:00,2013-07-12 12:00:00)",
      "(2013-07-12 12:00:00,2013-07-26 12:00:00)",
      "(2013-07-26 12:00:00,2013-08-09 12:00:00)"
    };
    
    PigTest test = new PigTest(script);
        
    test.assertOutput("times", input, "time_series_bounds", output);
  }
  

  
  @Test
  public void testAnchorLeftIncludeTarget() throws IOException, ParseException {
    final String[] script = {
      "times = LOAD 'input' USING PigStorage() AS (time: chararray);",
      "times_group = GROUP times ALL;",
      "times_bounds = FOREACH times_group GENERATE MIN(times.time) AS min_time, MAX(times.time) AS max_time;",
      "time_series_bounds = FOREACH times_bounds GENERATE FLATTEN(rsciudfs.GetIncrementedTimes(min_time, max_time, 'yyyy-MM-dd HH:mm:ss', 1209600, '-anchorLeft', '-includeTarget')) AS (time_step_lower_bound: chararray, time_step_upper_bound: chararray);",
      "STORE time_series_bounds INTO 'output';"
    };
    
    final String[] input = {
      "2013-04-05 12:00:00",
      "2013-08-15 16:00:00"
    };
    
    final String[] output = {
      "(2013-03-22 12:00:00,2013-04-05 12:00:00)",
      "(2013-04-05 12:00:00,2013-04-19 12:00:00)",
      "(2013-04-19 12:00:00,2013-05-03 12:00:00)",
      "(2013-05-03 12:00:00,2013-05-17 12:00:00)",
      "(2013-05-17 12:00:00,2013-05-31 12:00:00)",
      "(2013-05-31 12:00:00,2013-06-14 12:00:00)",
      "(2013-06-14 12:00:00,2013-06-28 12:00:00)",
      "(2013-06-28 12:00:00,2013-07-12 12:00:00)",
      "(2013-07-12 12:00:00,2013-07-26 12:00:00)",
      "(2013-07-26 12:00:00,2013-08-09 12:00:00)"
    };
    
    PigTest test = new PigTest(script);
        
    test.assertOutput("times", input, "time_series_bounds", output);
  }
  
  @Test
  public void testAnchorRight() throws IOException, ParseException {
    final String[] script = {
      "times = LOAD 'input' USING PigStorage() AS (time: chararray);",
      "times_group = GROUP times ALL;",
      "times_bounds = FOREACH times_group GENERATE MIN(times.time) AS min_time, MAX(times.time) AS max_time;",
      "time_series_bounds = FOREACH times_bounds GENERATE FLATTEN(rsciudfs.GetIncrementedTimes(min_time, max_time, 'yyyy-MM-dd HH:mm:ss', 1209600, '-anchorRight')) AS (time_step_lower_bound: chararray, time_step_upper_bound: chararray);",
      "STORE time_series_bounds INTO 'output';"
    };
    
    final String[] input = {
      "2013-04-05 12:00:00",
      "2013-08-15 16:00:00"
    };
    
    final String[] output = {
      "(2013-08-01 16:00:00,2013-08-15 16:00:00)",
      "(2013-07-18 16:00:00,2013-08-01 16:00:00)",
      "(2013-07-04 16:00:00,2013-07-18 16:00:00)",
      "(2013-06-20 16:00:00,2013-07-04 16:00:00)",
      "(2013-06-06 16:00:00,2013-06-20 16:00:00)",
      "(2013-05-23 16:00:00,2013-06-06 16:00:00)",
      "(2013-05-09 16:00:00,2013-05-23 16:00:00)",
      "(2013-04-25 16:00:00,2013-05-09 16:00:00)",
      "(2013-04-11 16:00:00,2013-04-25 16:00:00)"
    };
    
    PigTest test = new PigTest(script);
        
    test.assertOutput("times", input, "time_series_bounds", output);
  }
  
  @Test
  public void testAnchorRightIncludeTarget() throws IOException, ParseException {
    final String[] script = {
      "times = LOAD 'input' USING PigStorage() AS (time: chararray);",
      "times_group = GROUP times ALL;",
      "times_bounds = FOREACH times_group GENERATE MIN(times.time) AS min_time, MAX(times.time) AS max_time;",
      "time_series_bounds = FOREACH times_bounds GENERATE FLATTEN(rsciudfs.GetIncrementedTimes(min_time, max_time, 'yyyy-MM-dd HH:mm:ss', 1209600, '-anchorRight', '-includeTarget')) AS (time_step_lower_bound: chararray, time_step_upper_bound: chararray);",
      "STORE time_series_bounds INTO 'output';"
    };
    
    final String[] input = {
      "2013-04-05 12:00:00",
      "2013-08-15 16:00:00"
    };
    
    final String[] output = {
      "(2013-08-15 16:00:00,2013-08-29 16:00:00)",
      "(2013-08-01 16:00:00,2013-08-15 16:00:00)",
      "(2013-07-18 16:00:00,2013-08-01 16:00:00)",
      "(2013-07-04 16:00:00,2013-07-18 16:00:00)",
      "(2013-06-20 16:00:00,2013-07-04 16:00:00)",
      "(2013-06-06 16:00:00,2013-06-20 16:00:00)",
      "(2013-05-23 16:00:00,2013-06-06 16:00:00)",
      "(2013-05-09 16:00:00,2013-05-23 16:00:00)",
      "(2013-04-25 16:00:00,2013-05-09 16:00:00)",
      "(2013-04-11 16:00:00,2013-04-25 16:00:00)"
    };
    
    PigTest test = new PigTest(script);
        
    test.assertOutput("times", input, "time_series_bounds", output);
  }
  
  public void testDefaultAnchor() throws IOException, ParseException {
    final String[] script = {
      "times = LOAD 'input' USING PigStorage() AS (time: chararray);",
      "times_group = GROUP times ALL;",
      "times_bounds = FOREACH times_group GENERATE MIN(times.time) AS min_time, MAX(times.time) AS max_time;",
      "time_series_bounds = FOREACH times_bounds GENERATE FLATTEN(rsciudfs.GetIncrementedTimes(min_time, max_time, 'yyyy-MM-dd HH:mm:ss', 1209600)) AS (time_step_lower_bound: chararray, time_step_upper_bound: chararray);",
      "STORE time_series_bounds INTO 'output';"
    };
    
    final String[] input = {
      "2013-04-05 12:00:00",
      "2013-08-15 16:00:00"
    };
    
    final String[] output = {
      "(2013-04-05 12:00:00,2013-04-19 12:00:00)",
      "(2013-04-19 12:00:00,2013-05-03 12:00:00)",
      "(2013-05-03 12:00:00,2013-05-17 12:00:00)",
      "(2013-05-17 12:00:00,2013-05-31 12:00:00)",
      "(2013-05-31 12:00:00,2013-06-14 12:00:00)",
      "(2013-06-14 12:00:00,2013-06-28 12:00:00)",
      "(2013-06-28 12:00:00,2013-07-12 12:00:00)",
      "(2013-07-12 12:00:00,2013-07-26 12:00:00)",
      "(2013-07-26 12:00:00,2013-08-09 12:00:00)"
    };
    
    PigTest test = new PigTest(script);
        
    test.assertOutput("times", input, "time_series_bounds", output);
  }
}