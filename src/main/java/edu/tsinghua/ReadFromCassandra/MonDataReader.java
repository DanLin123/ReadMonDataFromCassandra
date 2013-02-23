/**
 * 
 * 从Cassandra数据库中读取监控数据
 * 
 * */

package edu.tsinghua.ReadFromCassandra;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.eaio.uuid.UUID;
import com.eaio.uuid.UUIDGen;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;



public class MonDataReader {
	String url;   //ip:thrift端口
	String clusterName;
	String keyspaceName;
	String columnFamilyName;
	Cluster cluster;
	Keyspace ksp;
	

	/**
	 * 指定要查询的url, clusterName, keyspace, columnFamily
	 * */
	public MonDataReader( String clusterName, String url, String keyspaceName, String columnFamilyName)
	{
		this.clusterName = clusterName;
		this.url = url;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
	}
	
	public void init()
	{

		//init cluster
		cluster = HFactory.getOrCreateCluster(this.clusterName, this.url);	
		System.out.println("Cluster instantiated");
		
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(this.keyspaceName);
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef == null) {
					ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(this.keyspaceName,                              
							columnFamilyName, 
		                    ComparatorType.BYTESTYPE);

					KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,                 
		                  ThriftKsDef.DEF_STRATEGY_CLASS,  
		                  1, 
		                  Arrays.asList(cfDef));
					//Add the schema to the cluster.
					//"true" as the second param means that Hector will block until all nodes see the change.
					cluster.addKeyspace(newKeyspace, true);
				}
		
		ksp = HFactory.createKeyspace(keyspaceName, cluster);
		System.out.println("Keyspace " +  keyspaceName + " instantiated");
		
	}
	
	/**
	 * @param keyspace,监控数据保存的keyspce
	 * @param columnFamily,监控数据保存的culumnFamily
	 * @param metric, host#plugin#pluginIntsance#type#typeInstance
	 * @param startTime, 查询的开始时间点
	 * @param endTime,查询的结束时间点
	 * */
	public ColumnSliceIterator<String, Long, Long> getDatas(String metric, long startTime, long endTime)
	{
		/*UUID startTime = new UUID(UUIDGen.createTime(startTimeInput),UUIDGen.getClockSeqAndNode());
		UUID endTime = new UUID(UUIDGen.createTime(endTimeInput),UUIDGen.getClockSeqAndNode());*/
		// Iterates over all columns for the row identified by key "a key"
		SliceQuery<String, Long, Long> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
	LongSerializer.get(), LongSerializer.get()).
		    setKey(metric).setColumnFamily(columnFamilyName);
		
		ColumnSliceIterator<String, Long, Long> iterator = new ColumnSliceIterator<String, Long, Long>(query, startTime, endTime, false);		
		return iterator;
	}
	
	/**
	 * get all the data from a row
	 * */
	/*public ColumnSliceIterator<String, UUID, String> getRowDatas(String metric)
	{
			
		SliceQuery<String, UUID, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
				TimeUUIDSerializer.get(), StringSerializer.get()).
				setKey(metric).setColumnFamily(columnFamilyName);
		
		ColumnSliceIterator<String, UUID, String> iterator = new ColumnSliceIterator<String, UUID, String>(query, null, null, false);		
		return iterator;
	}*/
	
	/**
	 * This will page through the column family in pages of 100 rows. 
	 * It will only fetch 10 columns for each row (you will want to page very long rows too).
	 * This is for a column family with uuids for row keys, 
	 * strings for column names and longs for values. 
	 * */
	public ArrayList<String> getKeys()
	{
		int row_count = 100;
		ArrayList<String> keys = new ArrayList<String>();
		
        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
            .createRangeSlicesQuery(ksp, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
            .setColumnFamily(columnFamilyName)
            .setRange(null, null, false, 10)
            .setRowCount(row_count);

        String last_key = null;

        while (true) {
            rangeSlicesQuery.setKeys(last_key, null);
            System.out.println(" > " + last_key);

            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> rows = result.get();
            Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

            // we'll skip this first one, since it is the same as the last one from previous time we executed
            if (last_key != null && rowsIterator != null) rowsIterator.next();   

            while (rowsIterator.hasNext()) {
              Row<String, String, String> row = rowsIterator.next();
              last_key = row.getKey();

              if (row.getColumnSlice().getColumns().isEmpty()) {
                continue;
              }


             keys.add(row.getKey());
            }

            if (rows.getCount() < row_count)
                break;
        }
        return keys;
	}

}
