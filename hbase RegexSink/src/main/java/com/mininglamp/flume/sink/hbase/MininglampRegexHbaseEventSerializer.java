/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.mininglamp.flume.sink.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An {@link HbaseEventSerializer} which parses columns based on a supplied
 * regular expression and column name list.
 * <p>
 * Note that if the regular expression does not return the correct number of
 * groups for a particular event, or it does not correctly match an event,
 * the event is silently dropped.
 * <p>
 * Row keys for each event consist of a timestamp concatenated with an
 * identifier which enforces uniqueness of keys across flume agents.
 * <p>
 * See static constant variables for configuration options.
 */
public class MininglampRegexHbaseEventSerializer implements HbaseEventSerializer {
  // Config vars
  /** Regular expression used to parse groups from event data. */
  public static final String REGEX_CONFIG = "regex";
  public static final String REGEX_DEFAULT = "(.*)";

  /** Whether to ignore case when performing regex matches. */
  public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
  public static final boolean INGORE_CASE_DEFAULT = false;

  /** Comma separated list of column names to place matching groups in. */
  public static final String COL_NAME_CONFIG = "colNames";
  public static final String COLUMN_NAME_DEFAULT = "payload";

  /** Index of the row key in matched regex groups */
  public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";

  /** Placeholder in colNames for row key */
  public static final String ROW_KEY_NAME = "ROW_KEY";

  /** Whether to deposit event headers into corresponding column qualifiers */
  public static final String DEPOSIT_HEADERS_CONFIG = "depositHeaders";
  public static final boolean DEPOSIT_HEADERS_DEFAULT = false;
  
  /** Set rowKeyPrefix or rowKeySuffix from where and value */
  public static final String ROEKEYPREFIX_CONFIG = "rowKeyPrefix";
  public static final String ROEKEYPREFIX_FROM = "rowKeyPrefixFrom";
  public static final String ROEKEYPREFIX_DEFAULT = "";
  
  public static final String ROEKEYSUFFIX_CONFIG = "rowKeySuffix";
  public static final String ROEKEYSUFFIX_FROM = "rowKeySuffixFrom";
  public static final String ROEKEYSUFFIX_DEFAULT = "";
  
  public static final String ISSETPREFIX_CONFIG = "setPrefix";
  public static final boolean ISSETPREFIX_SUFFIX_DEFAULT = false;

  public static final String ISSETSUFFIX_CONFIG = "setSuffix";
  public static final boolean ISSETSUFFIX_SUFFIX_DEFAULT = false;
  
  /**wheather rowkey to timestamp*/
  public static final String ROWKEYTOTIMESTAMP_CONFIG = "rowKeyToTimeStamp";
  public static final String ROWKEYTIMEFORMAT = "rowKeyFormat";
  public static final boolean ROWKEYTOTIMESTAMP_DEFAULT = false;
  
  /** What charset to use when serializing into HBase's byte arrays */
  public static final String CHARSET_CONFIG = "charset";
  public static final String CHARSET_DEFAULT = "UTF-8";

  /* This is a nonce used in HBase row-keys, such that the same row-key
   * never gets written more than once from within this JVM. */
  protected static final AtomicInteger nonce = new AtomicInteger(0);
  protected static String randomKey = RandomStringUtils.randomAlphanumeric(10);
  protected byte[] cf;
  private byte[] payload;
  private List<byte[]> colNames = Lists.newArrayList();
  private Map<String, String> headers;
  private boolean regexIgnoreCase;
  private boolean depositHeaders;
  private Pattern inputPattern;
  private Charset charset;
  private int rowKeyIndex;
  private boolean isSetPrefix;
  private boolean isSetSuffix;
  private String rowKeyPrefix;
  private String rowKeySuffix;
  private XXFixFromType rowKeyPrefixFrom = XXFixFromType.notype;
  private XXFixFromType rowKeySuffixFrom = XXFixFromType.notype;
  private boolean rowKeyToTimestamp;
  private String rowKeyTimeFormat;
  
  public void configure(Context context) {

    String regex = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
    regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,
        INGORE_CASE_DEFAULT);
    depositHeaders = context.getBoolean(DEPOSIT_HEADERS_CONFIG,
        DEPOSIT_HEADERS_DEFAULT);
    inputPattern = Pattern.compile(regex, Pattern.DOTALL
        + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    charset = Charset.forName(context.getString(CHARSET_CONFIG,
        CHARSET_DEFAULT));

    String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
    String[] columnNames = colNameStr.split(",");
    for (String s : columnNames) {
      colNames.add(s.getBytes(charset));
    }
    
    rowKeyToTimestamp = context.getBoolean(ROWKEYTOTIMESTAMP_CONFIG,ROWKEYTOTIMESTAMP_DEFAULT);
    rowKeyTimeFormat = context.getString(ROWKEYTIMEFORMAT);
    if(rowKeyToTimestamp)
    {
    	if(rowKeyTimeFormat== null || rowKeyTimeFormat.equals(""))
    	{
    		throw new IllegalArgumentException("if set rowKeyToTimestamp rowKeyTimeFormat can not be " +
    	            "null ");
    	}
    }
    //Rowkey is optional, default is -1
    rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
    //if row key is being used, make sure it is specified correct
    if (rowKeyIndex >= 0) {
      if (rowKeyIndex >= columnNames.length) {
        throw new IllegalArgumentException(ROW_KEY_INDEX_CONFIG + " must be " +
            "less than num columns " + columnNames.length);
      }
      if (!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
        throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be "
            + ROW_KEY_NAME + " and is " + columnNames[rowKeyIndex]);
      }
    }
    InitPreAndSuffixType(context,columnNames);
  }

  private void InitPreAndSuffixType(Context context,String[] columnNames)
  {
	  isSetPrefix = context.getBoolean(ISSETPREFIX_CONFIG,ISSETPREFIX_SUFFIX_DEFAULT);
	  isSetSuffix = context.getBoolean(ISSETSUFFIX_CONFIG,ISSETSUFFIX_SUFFIX_DEFAULT);
	  if(isSetPrefix){
		  rowKeyPrefix = context.getString(ROEKEYPREFIX_CONFIG, ROEKEYPREFIX_DEFAULT);
		  rowKeyPrefixFrom = XXFixFromType.getTypeFromString(context.getString(ROEKEYPREFIX_FROM,""));
		  if(rowKeyPrefixFrom.equals(XXFixFromType.body))
		  {
			  rowKeyPrefix = fillBodyPrefixOrSuffixValue(rowKeyPrefixFrom,rowKeyPrefix,columnNames);
		  }
	  }
	  if(isSetSuffix)
	  {
		  rowKeySuffix = context.getString(ROEKEYSUFFIX_CONFIG, ROEKEYSUFFIX_DEFAULT);
		  rowKeySuffixFrom = XXFixFromType.getTypeFromString(context.getString(ROEKEYSUFFIX_FROM,""));
		  if(rowKeySuffixFrom.equals(XXFixFromType.body))
		  {
			  rowKeySuffix = fillBodyPrefixOrSuffixValue(rowKeySuffixFrom,rowKeySuffix,columnNames);
		  }
	  }
  }
  private String fillBodyPrefixOrSuffixValue(XXFixFromType type,String XXFixValue,String[] columnNames){
	  int length = columnNames.length;
		boolean isFind = false;
		for (int i = 0; i < length; i++) {
			if(columnNames[i].equals(XXFixValue))
			{
				XXFixValue = String.valueOf(i);
				isFind = true;
				break;
			}
		}
		if(!isFind)
		{
			throw new IllegalArgumentException(XXFixValue + " is not in Column ");
		}
		return XXFixValue;
  }
  private String getPreAndSuffixValue(XXFixFromType type,String XXFixValue,Matcher m)
  {
	  String value = "";
	  switch (type) {
	  case body:
		  value = m.group(Integer.valueOf(XXFixValue) + 1);
		break;
	  case self:
		  value = XXFixValue;
			break;
	  case head:
		  if(isSetPrefix && rowKeyPrefixFrom.equals(XXFixFromType.head)){
			  if(rowKeyPrefix !=null && headers.containsKey(rowKeyPrefix)){
				  value = headers.get(rowKeyPrefix);
			  }else{
				  throw new IllegalArgumentException(rowKeyPrefix + " is not in Head ");
			  }
		  }
		  if(isSetSuffix && rowKeySuffixFrom.equals(XXFixFromType.head)){
			  if(rowKeySuffix !=null && headers.containsKey(rowKeySuffix)){
				  value = headers.get(rowKeySuffix);
			  }else{
				  throw new IllegalArgumentException(rowKeySuffix + " is not in Head ");
			  }
		  }
			break;
	  default:
		break;
	}
	  return value;
  }
  
  public void configure(ComponentConfiguration conf) {
  }

  public void initialize(Event event, byte[] columnFamily) {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
    this.cf = columnFamily;
  }

  /**
   * Returns a row-key with the following format:
   * [time in millis]-[random key]-[nonce]
   */
  protected byte[] getRowKey(Calendar cal) {
    /* NOTE: This key generation strategy has the following properties:
     * 
     * 1) Within a single JVM, the same row key will never be duplicated.
     * 2) Amongst any two JVM's operating at different time periods (according
     *    to their respective clocks), the same row key will never be 
     *    duplicated.
     * 3) Amongst any two JVM's operating concurrently (according to their
     *    respective clocks), the odds of duplicating a row-key are non-zero
     *    but infinitesimal. This would require simultaneous collision in (a) 
     *    the timestamp (b) the respective nonce and (c) the random string.
     *    The string is necessary since (a) and (b) could collide if a fleet
     *    of Flume agents are restarted in tandem.
     *    
     *  Row-key uniqueness is important because conflicting row-keys will cause
     *  data loss. */
    String rowKey = String.format("%s-%s-%s", cal.getTimeInMillis(),
        randomKey, nonce.getAndIncrement());
    return rowKey.getBytes(charset);
  }

  protected byte[] getRowKey() {
    return getRowKey(Calendar.getInstance());
  }

  private byte[] setRowKey(byte[] rowKey,Matcher m)
  {
	  String newRowKey = new String(rowKey,charset);
	
	  if(isSetPrefix)
      {
		  String prefix = getPreAndSuffixValue(rowKeyPrefixFrom, rowKeyPrefix, m);
    	  if(!rowKeyPrefix.equals(""))
    		  newRowKey = prefix + "-" + newRowKey;
      }
	  if(isSetSuffix)
	  {
		  String suffix = getPreAndSuffixValue(rowKeySuffixFrom, rowKeySuffix, m);
    	  if(!rowKeySuffix.equals(""))
    		  newRowKey = newRowKey + "-" + suffix;
	  }
	  
	  
	  return newRowKey.getBytes();
  }
  
  @SuppressWarnings("deprecation")
public List<Row> getActions() throws FlumeException {
    List<Row> actions = Lists.newArrayList();
    byte[] rowKey;
    Matcher m = inputPattern.matcher(new String(payload, charset));
    if (!m.matches()) {
      return Lists.newArrayList();
    }

    if (m.groupCount() != colNames.size()) {
      return Lists.newArrayList();
    }

    try {
      if (rowKeyIndex < 0) {
        rowKey = getRowKey();
      } else {
    	  if(rowKeyToTimestamp){
    		  String rowkeyStr = m.group(rowKeyIndex + 1);
    		  Calendar c = Calendar.getInstance();  
    		  c.setTime(new SimpleDateFormat(rowKeyTimeFormat).parse(rowkeyStr));  
    		  rowKey = String.valueOf(c.getTimeInMillis()).getBytes(Charsets.UTF_8);
    	  }else{
    		  rowKey = m.group(rowKeyIndex + 1).getBytes(Charsets.UTF_8);
    	  }
      }
      rowKey = setRowKey(rowKey,m);
      Put put = new Put(rowKey);

      for (int i = 0; i < colNames.size(); i++) {
        if (i != rowKeyIndex) {
          put.add(cf, colNames.get(i), m.group(i + 1).getBytes(Charsets.UTF_8));
        }
      }
      if (depositHeaders) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          put.add(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
        }
      }
      actions.add(put);
    } catch (Exception e) {
      throw new FlumeException("Could not get row key!", e);
    }
    return actions;
  }

  public List<Increment> getIncrements() {
    return Lists.newArrayList();
  }

  public void close() {
  }
  
  /**
   * 前缀后缀来源
   * @author Administrator
   *
   */
  public enum XXFixFromType{
	  notype,
	  body,
	  self,
	  head;
	  public static XXFixFromType getTypeFromString(String type)
	  {
		  if(type.equals("body"))
		  {
			  return body;
		  }
		  else if(type.equals("self"))
		  {
			  return self;
		  }else if(type.equals("head"))
		  {
			  return head;
		  }
		  return notype;
	  }
  }
}