package com.sujoy.test;

import org.junit.*;

import com.sujoy.arrays.AngleHourMinutes;

import static org.junit.Assert.*;
 
public class AngleHourTest {
    @Test
    public void testAngle1(){
        AngleHourMinutes a = new AngleHourMinutes();
        assertEquals(360, a.getangle(24, 00),0.1);      
        }
 
    @Test
    public void testAngle2(){
    	AngleHourMinutes a = new AngleHourMinutes();
        assertEquals(18, a.getangle(23, 58),0.1);
    }
    
    @Test
    public void testAngle3(){
    	AngleHourMinutes a = new AngleHourMinutes();
        assertEquals(18, a.getangle(-23, -58),0.1);
    }
}