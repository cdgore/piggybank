package com.cdgore;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;

import org.junit.Assert;
import org.junit.Test;

public class JsonKVPairsToBagTest {
	private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  
  @Test
  public void testExec() throws IOException {
    final JsonKVPairsToBag jsonKVPairsToBag = new JsonKVPairsToBag();
    
    final Tuple input = JsonKVPairsToBagTest.TUPLE_FACTORY.newTuple(1);
    input.set(0, "{\"key1\": \"1.1\", \"key2\": \"2.2\", \"key3\": \"3.3\"}");
    
    final DataBag result = jsonKVPairsToBag.exec(input);
    
    for (Tuple tup : result) {
      String key = (String) tup.get(0);
      double value = Double.parseDouble((String) tup.get(1));
      String errorMessage = "Value is wrong";
      
      if (key.equals("key1"))
        Assert.assertEquals(errorMessage, 1.1, value, 0.0);
      else if (key.equals("key2"))
        Assert.assertEquals(errorMessage, 2.2, value, 0.0);
      else if (key.equals("key3"))
        Assert.assertEquals(errorMessage, 3.3, value, 0.0);
    }
  }
  
  @Test(expected = IOException.class)
  public void testExecNullInput() throws IOException {
    final JsonKVPairsToBag jsonKVPairsToBag = new JsonKVPairsToBag();
    
    jsonKVPairsToBag.exec(null);
  }
}