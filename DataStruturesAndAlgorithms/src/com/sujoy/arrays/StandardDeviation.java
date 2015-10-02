package com.sujoy.arrays;

public class StandardDeviation {
public static void mani(String [] arr){
	int[] a1 = {2,4,5,1,0};
	System.out.println("The Standard Deviation of the array is: "+getStandardDeviation(a1));
}

public static double getStandardDeviation(int[] a1){
	double std=0;
	int sum1 = 0;
	float sum2=0;
	float mean=0;
	int length = a1.length;
	for(int x:a1){
		sum1 = sum1+x;
	}
	mean = sum1/length;
	
	for(int x: a1){
		sum2 = sum2+ (x-mean)*(x-mean);
	}
	std = Math.sqrt(sum2);
	return std;
}
}
