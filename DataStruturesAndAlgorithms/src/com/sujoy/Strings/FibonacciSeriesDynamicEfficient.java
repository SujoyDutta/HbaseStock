package com.sujoy.Strings;

public class FibonacciSeriesDynamicEfficient {
	// 0 1 1 2 3 5 8 13

	public static void main(String[] args) {

		int limit = 5; // user given input to limit the fibonacci series
		int[] memory = new int[limit];
		for (int i = 0; i < limit; i++)
			System.out.println(fibonacci(i, memory));
	}

	public static int fibonacci(int n, int[] memory) {

		// since memory lookup is constant time here,  O(n) will be the running
		// time for n items each having a constant lookup.
		if (n <= 0)
			return 0;
		else if (n == 1)
			return 1;
		else if (memory[n] > 0)
			return memory[n];

		memory[n] = fibonacci(n - 1, memory) + fibonacci(n - 2, memory);
		return memory[n];
	}
}
