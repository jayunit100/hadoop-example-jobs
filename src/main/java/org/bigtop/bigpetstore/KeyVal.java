package org.bigtop.bigpetstore;

public class KeyVal<K, V> {

	public final K key;
	public final V val;
	
	public KeyVal(K key, V val) {
		this.key = key;
		this.val = val;
	}
}
