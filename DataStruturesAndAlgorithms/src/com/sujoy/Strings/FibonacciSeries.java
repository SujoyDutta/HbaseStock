package com.sujoy.Strings;

public class FibonacciSeries {
//0 1 1 2 3 5 8 13
	
	public static void main(String [] args){
		
		int limit = 5; // user given input to limit the fibonacci series
		fibonacci(limit);
	}
	
	public static void fibonacci(int limit){
		int sum=0;
		int a = 0;
		int b = 1 ;
		if (limit == 0)
			System.out.println("No numbers input");
		else {
			System.out.println(sum);
		}
		
		
	}
}
