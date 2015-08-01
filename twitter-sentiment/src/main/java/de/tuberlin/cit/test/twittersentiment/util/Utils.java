package de.tuberlin.cit.test.twittersentiment.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class Utils {
	/**
	 * Sorts a map by the entries using the comparator.
	 *
	 * @param unsortedMap
	 * 		the map to sort
	 * @param comparator
	 * 		the comparator
	 * @return the new sorted map
	 */
	public static <K, V> Map<K, V> sortMapByEntry(Map<K, V> unsortedMap, Comparator<Map.Entry<K, V>> comparator) {
		LinkedList<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(unsortedMap.entrySet());

		Collections.sort(list, comparator);

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	/**
	 * Returns a map containing the first n entries of the given map or the whole map if the size of the map is lower than n.
	 *
	 * @param src
	 * 		the map to slice (source)
	 * @param n
	 * 		the maximum size for the new map
	 * @return a map containing the first n entries of the given map or the whole map if the size of the map is lower than n
	 */
	public static <K, V> Map<K, V> slice(Map<K, V> src, int n) {
		return slice(src, new LinkedHashMap<K, V>(), n);
	}

	/**
	 * Returns a map containing the first n entries of the given map or the whole map if the size of the map is lower than n.
	 *
	 * @param src
	 * 		the map to slice (source)
	 * @param dst
	 * 		the result map (destination)
	 * @param n
	 * 		the maximum size for the new map
	 * @return a map containing the first n entries of the given map or the whole map if the size of the map is lower than n
	 */
	public static <K, V> Map<K, V> slice(Map<K, V> src, Map<K, V> dst, int n) {
		Iterator<Map.Entry<K, V>> iterator = src.entrySet().iterator();
		for (int i = 0; i < n && iterator.hasNext(); i++) {
			Map.Entry<K, V> entry = iterator.next();
			dst.put(entry.getKey(), entry.getValue());
		}
		return dst;
	}
}
