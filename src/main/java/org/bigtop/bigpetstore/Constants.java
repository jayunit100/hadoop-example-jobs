package org.bigtop.bigpetstore;

/**
 * Globals used by the app
 */
public class Constants {

	/**
	 * Each "state" has a pet store , with a certain 
	 * "proportion" of the transactions.  
	 * In this case colorado represents the majority 
	 * of the transactions.
	 */
	public enum STATE{
		AZ(.1f),
		AK(.2f),
		CT(.2f),
		OK(.1f),
		CO(.4f);
		
		float probability;
		private STATE(float probability) {
			this.probability = probability;
		}
	}
	
}
