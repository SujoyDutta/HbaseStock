package com.sujoy.Strings;

import java.util.HashMap;
import java.util.Map;

public class Anagrams {

	public static void main(String [] args){
		//brute force way is to sort the alphabets in words and compare Array.equals(w1,w2) but it is O(n^2)
		// better solution is to cover through counts as decribed in solution O(n)
		String s1 = "cat";
		String s2 = "act";
		if(isAnagram(s1,s2))
			System.out.println(s1 +" and "+s2+" are anagrams");
		else
			System.out.println(s1 +" and "+s2+" are Not Anagrams");
	}
	
	public static boolean isAnagram(String s1, String s2){
	 
		// counting the frequency of each alphabet of the string, incrementing it for word1 and decrementing for word2 
		// so that finally there is all 0's for the chars if both are anagrams
		char[] word1 = s1.toLowerCase().replaceAll("\\W", "").toCharArray();
		char[] word2 = s2.toLowerCase().replaceAll("\\W", "").toCharArray();
		Map<Character, Integer> anagramMap = new HashMap<Character, Integer>();
		
		int count;
		for (char c: word1) {
			count= anagramMap.containsKey(c)?anagramMap.get(c):0;
			anagramMap.put(c, count+1);
		}
		
		for (char c : word2) {
			 count = anagramMap.containsKey(c)?anagramMap.get(c):0;
			 anagramMap.put(c, count-1);
		}
		for (char c: anagramMap.keySet()) {
			 if(anagramMap.get(c)!= 0)
				 return false;
		}
		
		return true;
	}
}
