package com.wpi.cs4432.simpledb.tests.performance;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;


public class CreateTestTables 
{
	public static void createTestTables(int numEntries, Connection conn)
	{
		Random rand = null;
		Statement s = null;
		try
		{
			s=conn.createStatement();
			s.executeUpdate("Create table test1 (a11 int, a12 int)");
			s.executeUpdate("Create table test2 (a21 int, a22 int)");
			s.executeUpdate("Create table test3 (a31 int, a32 int)");
			s.executeUpdate("Create table test4 (a41 int, a42 int)");
			s.executeUpdate("Create table test5 (a51 int, a52 int)");
		
			s.executeUpdate("create sh index idx1 on test1 (a11)");
			//s.executeUpdate("create ex index idx2 on test2 (a21)");
			s.executeUpdate("create bt index idx3 on test3 (a31)");
		
			for(int i=1;i<6;i++)
			{
				/*if(i!=5)
				{*/
					rand=new Random(1);// ensure every table gets the same data
					for(int j=0;j<numEntries;j++)
					{
						s.executeUpdate("insert into test"+i+" (a"+i+"1,a"+i+"2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
					}
				//}
				/*else//case where i=5
				{
					for(int j=0;j<numEntries/2;j++)// insert 10000(half the size) records into test5
					{
						s.executeUpdate("insert into test"+i+" (a51,a52) values("+j+","+j+ ")");
					}
				}*/
		   }
		} 
		catch (SQLException e)
		{
			System.out.println("Issue creating test tables:");
			e.printStackTrace();
		}
	}
}

