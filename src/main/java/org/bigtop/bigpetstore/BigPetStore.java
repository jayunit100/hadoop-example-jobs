package org.bigtop.bigpetstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class BigPetStore {

	public static void main(String[] args) throws Exception {
		/**
  	 	 * Part one: ETL of large, semistructured data into the
  	 	 * cluster.  
		 */
		Configuration c = new Configuration();
		c.setInt("totalRecords", 100);
		Job createInput = PetStoreJob.
				createJob(new Path("petstoredata"), c);
		createInput.waitForCompletion(true);

		/**
		 * Part two: Basic analytics using pig and hive. 
		 */
		
	}
}
