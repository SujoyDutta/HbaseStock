package com.sujoy.arrays;


public class AngleHourMinutes {

	public static void main(String[] args) {
		System.out
				.println("Enter the time format in hours:min with hours between 0 and 23");
		String[] time = args[0].split(":");
		//String t1="23:58";
		//String t1 = "24:00";
		//String[] time = t1.split(":");
		int hours = Integer.parseInt(time[0]);
		int minutes = Integer.parseInt(time[1]);
		System.out.println("The angle between H and M is :"
				+ getangle(hours, minutes)+" degrees");
	}

	public static float getangle(int hours, int minutes) {
		float angle = 0;
		// the idea is to get the difference of hour and minutes angle wrt '12' 
		if(minutes==0)
			minutes = 60;
		float minute_angle = 360*Math.abs(minutes)/60;
		float hours_angle = 360*(Math.abs(hours%12))/12; // %12 is included to accommodate 12-23 hours
		//returning the difference between the two will the angle
	    angle = (minute_angle>hours_angle)?minute_angle-hours_angle:hours_angle-minute_angle;
		return angle ;
	}
}
