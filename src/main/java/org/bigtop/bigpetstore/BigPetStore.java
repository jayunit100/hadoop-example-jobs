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
		Job createInput = PetStoreTransactionGeneratorJob.
				createJob(new Path("petstoredata"), new Configuration());
		createInput.waitForCompletion(true);

		/**
		 * Part two: Basic analytics using pig and hive. 
		 */
		
	}
}