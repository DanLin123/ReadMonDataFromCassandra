package edu.tsinghua.ReadFromCassandra;

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
    	MonDataReader test = new MonDataReader("Test Cluster", "localhost:9160");
    	ColumnSliceIterator<String, String, String>  iterator = 
    			test.getDatas("RawData", "Time", "ld-VirtualBox#processes##ps_state#sleeping",  "135928455", "135928485");
    	
    	while(iterator.hasNext())
		{
			HColumn column = iterator.next();
			System.out.println(column.getName()+ "  "+column.getValue());
		}
    }
 
}
