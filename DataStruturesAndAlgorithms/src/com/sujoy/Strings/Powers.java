package com.sujoy.Strings;

public class Powers {

	public static void main(String[] args) {

		System.out.println(power(5, 3));
	}

/*	public static int power(int base, int exp) {
       // this has O(n) time , we can improve upto log(n)
		if (exp == 0)
			return 1;
		return base * power(base, --exp);
	}
*/	
	public static int power(int base, int exp){
		if(exp ==0)
			return 1;
		if (exp ==1)
			return base;
	    if(exp%2==0){
	    	int res = power(base,exp/2);
	    	return res*res;//asit was split into two
	    }
	    else{
	    	int res = power(base, (exp-1)/2);
	    	return base*res*res;
	    }
	
	}
}
