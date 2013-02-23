package edu.tsinghua.ReadFromCassandra;
import com.eaio.uuid.UUID;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for monDataReader
 */
public class MonDataReaderTest extends TestCase
{
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MonDataReaderTest( String testName )
    {
        super( testName );
       
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MonDataReaderTest.class );
    }
    
    /**
     * test getDatas funciton
     * */
   public void testGetDatas()
    {
    	MonDataReader test = new MonDataReader("Test Cluster", "127.0.0.1:9160","Monitor","RawData");
    	test.init();
    	
    	ColumnSliceIterator<String, Long, Long>  iterator = 
    			test.getDatas("ld-VirtualBox#interface#eth0#if_errors##2013-02-22",
    					System.currentTimeMillis()/1000-100, System.currentTimeMillis());
    	
    	
    	while(iterator.hasNext())
		{
			HColumn column = iterator.next();
			System.out.println( column.getName() + "  "+column.getValue());
		}
    }
    
    /**
     * test getRowDatas funciton:get all columns for one row
     * */ 
   /* public void testGetRowDatas()
    {
    	MonDataReader test = new MonDataReader("Test Cluster", "127.0.0.1:9160","Monitor","RawData");
    	test.init();
    	
    	ColumnSliceIterator<String, String, String>  iterator = 
    			test.getRowDatas("ld-VirtualBox#interface#eth0#if_octets##2013-02-22");
    	
    	while(iterator.hasNext())
		{
			HColumn column = iterator.next();
			System.out.println(column.getName()+ "  "+column.getValue());
		}
    }*/
    
//    org.apache.cassandra.db:type=ColumnFamilies,keyspace=RawData,columnfamily=Time
   /* public void testGetKeys()
    {
    	MonDataReader test = new MonDataReader("Test Cluster", "127.0.0.1:9160","Monitor","RawData");
    	test.init();
    	ArrayList<String> keys = test.getKeys();
    	
    	//wirte to file
    	// Create file 
    	
		try {
			FileWriter fstream = new FileWriter("/home/ld/collectdPythonCode/keys");
			BufferedWriter out = new BufferedWriter(fstream);
	    	out.write("-----------keys-----------------");
	    	for(String key : keys)
	    	{
	    		out.append(key+"\n");
	    		System.out.println(key);
	    	}
	    	out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }*/
   
	public static java.util.UUID toUUID(byte[] uuid) {
		long msb = 0;
		long lsb = 0;
		assert uuid.length == 16;
		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (uuid[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (uuid[i] & 0xff);
		long mostSigBits = msb;
		long leastSigBits = lsb;

		com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb, lsb);
		return java.util.UUID.fromString(u.toString());
	}
 
}
