package com.wpi.cs4432.simpledb.tests.performance;

import java.sql.SQLException;
import java.util.Date;

import org.junit.Test;

import simpledb.exploit.ExploitSortQueryPlanner;
import simpledb.server.SimpleDB;

import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;

public class SortMergeJoinTests extends SimpleDBBaseTest
{
	@Test
	public void testTwoJoins() throws SQLException
	{
		long start, after1, after2 = 0;
		start = new Date().getTime();
		stmt.executeQuery("select a41, a42, a51, a52 from test4, test5 where a41 = a51");
		after1 = new Date().getTime();
		stmt.executeQuery("select a41, a42, a51, a52 from test4, test5 where a41 = a51");
		after2 = new Date().getTime();
		
		System.out.print("{\"time1\": ");
		System.out.print(after1-start);
		System.out.print(", \"time2\": ");
		System.out.print(after2-after1);
		System.out.print(" }\n");
	}
	

}
