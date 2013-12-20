package com.cdgore.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.JsonParseException;

import org.apache.hadoop.io.Text;

/**
 * Accepts a JSON chararray containing {key: value} pairs and returns a DataBag containing
 * Tuples of the key value pairs. For example:
 *
 * INPUT:
 * {"email_open" : "0.014909921373024116", "email_click" : "0.013381788642734023", "click" : "0.032996975306163016", "purchase" : "7.7345610760846455", "shopping_cart" : "2.3260353693494427", "view" : "0.7275636800328681"}
 *
 * OUTPUT:
 * (email_open,0.014909921373024116)
 * (email_click,0.013381788642734023)
 * (click,0.032996975306163016)
 * (purchase,7.7345610760846455)
 * (shopping_cart,2.3260353693494427)
 * (view,0.7275636800328681)
 */

public class JsonKVPairsToBag extends EvalFunc<DataBag> {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  
  private final JsonFactory JSON_FACTORY = new JsonFactory();
    
  @Override
  public DataBag exec(final Tuple input) throws IOException {
    if (input == null)
      throw new IOException("Input is null");
    
    final DataBag resultBag = JsonKVPairsToBag.BAG_FACTORY.newDefaultBag();
    
    final Object inputString = input.get(0);
    
    if (!(inputString instanceof String))
      throw new IOException("Expected input to be chararray, but got " + inputString.getClass().getName());
    
    final String iString = (String) inputString;
    
    try {
      // trims null characters generated from JSONs in UTF-16.  This is necessary for
      // JSON objects outputted from Spark as of 0.7.0
      final JsonParser jP = JSON_FACTORY.createJsonParser(iString.replaceAll("\\s","").replaceAll("\\\0","").trim());  
    
      jP.nextToken();
      while (jP.nextToken() != JsonToken.END_OBJECT) {
        String keyName = jP.getCurrentName();
        jP.nextToken();
      
        final Tuple returnTuple = JsonKVPairsToBag.TUPLE_FACTORY.newTuple(2);
        returnTuple.set(0, keyName);
        returnTuple.set(1, jP.getText());
      
        resultBag.add(returnTuple);
      }
    } catch (JsonParseException e) {
      System.err.println("Failed to parse JSON: " + iString + "\n" + e);
    }
    
    return resultBag;
  }
}
