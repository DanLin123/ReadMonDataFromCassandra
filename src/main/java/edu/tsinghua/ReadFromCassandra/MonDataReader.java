/**
 * 从Cassandra数据库中读取监控数据
 * 如果无法连接，返回提示
 * 如果没有keyspace,返回提示
 * 
 * */

package edu.tsinghua.ReadFromCassandra;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;


public class MonDataReader {
	String url;   //ip:thrift端口
	String clusterName;    

	public MonDataReader( String clusterName, String url)
	{
		this.clusterName = clusterName;
		this.url = url;
		
	}
	
	/**
	 * @param keyspace,监控数据保存的keyspce
	 * @param columnFamily,监控数据保存的culumnFamily
	 * @param metric, host#plugin#pluginIntsance#type#typeInstance
	 * @param startTime, 查询的开始时间点
	 * @param endTime,查询的结束时间点
	 * */
	public ColumnSliceIterator<String, String, String> getDatas(String keyspace, String columnFamily, String metric, String startTime, String endTime)
	{
		//init cluster
		Cluster cluster = HFactory.getOrCreateCluster(this.clusterName, this.url);	
		System.out.println("Cluster instantiated");
		
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace);
		
		// If keyspace does not exist, the CFs don't exist either. => create them.
		if (keyspaceDef == null) {
			ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace",                              
                    "ColumnFamilyName", 
                    ComparatorType.BYTESTYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace",                 
                  ThriftKsDef.DEF_STRATEGY_CLASS,  
                  1, 
                  Arrays.asList(cfDef));
			//Add the schema to the cluster.
			//"true" as the second param means that Hector will block until all nodes see the change.
			cluster.addKeyspace(newKeyspace, true);
		}
		
		Keyspace ksp = HFactory.createKeyspace(keyspace, cluster);
		System.out.println("Keyspace " +  keyspace + " instantiated");
		
		// Iterates over all columns for the row identified by key "a key"
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
		    StringSerializer.get(), StringSerializer.get()).
		    setKey(metric).setColumnFamily(columnFamily);
		
		ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, startTime, endTime, false);		
		return iterator;
	}
	


}
