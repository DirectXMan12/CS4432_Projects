package com.wpi.cs4432.simpledb.tests;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


public class CreateTestTables 
{
	public static void createTestTables(int numEntries, Connection conn, boolean addRecords)
	{
		Random rand = null;
		Statement s = null;
		try
		{
			s=conn.createStatement();
			
			if (!addRecords) for (int i = 1; i < 6; i++) s.executeUpdate("Create table test"+i+" (a"+i+"1 int, a"+i+"2 int)");
			
			if (!addRecords)
			{
			//	createIndicies(s);
			}
			
			Random randomSeed = new Random();
			int seed = randomSeed.nextInt(20000);
		
			for(int i=1;i<6;i++)
			{
					rand=new Random(seed);// ensure every table gets the same data
					System.out.println("creating table "+i);
					for (int k = 0; k < 100; k++)
					{
						for(int j=0;j<numEntries/100;j++)
						{
							s.executeUpdate("insert into test"+i+"(a"+i+"1,a"+i+"2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ");");
						}
						System.out.println("created up through row "+(k+1)*numEntries/100);
					}
		   }
		} 
		catch (SQLException e)
		{
			System.out.println("Issue creating test tables:");
			e.printStackTrace();
		}
	}

	public static void createIndicies(Statement s) throws SQLException
	{
		s.executeUpdate("create sh index idx1 on test1 (a11)");
		s.executeUpdate("create eh index idx2 on test2 (a21)");
		s.executeUpdate("create bt index idx3 on test3 (a31)");
	}
	
	public static void addNRows(int numEntries, Connection conn)
	{
		Random rand = null;
		Statement s = null;
		try
		{
			s=conn.createStatement();
			
			Random randomSeed = new Random();
			int seed = randomSeed.nextInt(20000);
		
			for(int i=1;i<6;i++)
			{
				rand=new Random(seed);// ensure every table gets the same data
				for(int j=0;j<numEntries;j++)
				{
					s.executeUpdate("insert into test"+i+"(a"+i+"1,a"+i+"2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ");");
				}
		   }
		} 
		catch (SQLException e)
		{
			System.out.println("Issue creating test tables:");
			e.printStackTrace();
		}
	}
}

