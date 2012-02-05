/**
 * 
 */
package com.wpi.cs4432.simpledb.tests.performance;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.LRUBasicBufferMgr;
import simpledb.buffer.MRUBasicBufferMgr;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.studentClient.CreateStudentDB;

/**
 * @author directxman12
 *
 */
public class BufferMgrPerfomanceTests
{
	static Thread serverThread;
	static Driver mainDriver;
	static Connection mainConnection;
	Statement stmt;
	
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		try
		{
			LocateRegistry.createRegistry(1099); // default
		}
		catch(Exception ex)
		{
			
		}
		
		serverThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try
				{
					BufferMgr.setBasicBuffMgrType(LRUBasicBufferMgr.class);
					// configure and initialize the database
					SimpleDB.init("studentdb");
					System.out.println("finished init of SimpleDB");
					// post the server entry in the rmi registry
					RemoteDriver d = new RemoteDriverImpl();
					Naming.rebind("simpledb", d);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		
		serverThread.start();	
		
		mainDriver = new SimpleDriver();
		String connURL = "jdbc:simpledb://localhost";
		boolean b = true;
		while(b)
		{
			try
			{
				mainConnection = mainDriver.connect(connURL, null);
				
				b = false;
			}
			catch(Exception ex)
			{
				continue;
			}
		}
		
		Statement stmt = mainConnection.createStatement();
		try
		{
			stmt.executeQuery("Select SId from STUDENT");
		}
		catch(SQLException ex)
		{
			CreateStudentDB.main(new String[] {});
		}
	}
	
	@Test
	public void testInitConn() throws SQLException
	{
		stmt.executeQuery("select SId, SName, MajorId, GradYear from STUDENT");
	}
	
	@Test
	public void testStudentDBSelectPerf() throws SQLException
	{
		for(int i = 1; i < 1000; i++)
		{
			stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT;");
		}
	}
	
	@Test
	public void testStudentDBSortPerf() throws SQLException
	{
		BufferMgr.setBasicBuffMgrType(BasicBufferMgr.class);
		SimpleDB.bufferMgr().resetBasicBufferMgr();
		
		Random r = new Random();
		for (int i = 1; i < 1000; i++)
		{
			//stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT order by GradYear DESC;");
			//stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT order by MajorId DESC;");
			int res = r.nextInt(3);
			if (res == 0) stmt.executeQuery("SELECT SId, SName from STUDENT;");
			if (res == 1) stmt.executeQuery("SELECT SId, SName, GradYear from STUDENT where GradYear = 2004;");
			if (res == 2) stmt.executeQuery("SELECT SId, SName, MajorId from STUDENT order by MajorId DESC;");
		}
	}
	
	@Test
	public void testStudentDBLRUSortPerf() throws SQLException
	{
		BufferMgr.setBasicBuffMgrType(LRUBasicBufferMgr.class);
		SimpleDB.bufferMgr().resetBasicBufferMgr();
		Random r = new Random();
		for (int i = 1; i < 1000; i++)
		{
			//stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT order by GradYear DESC;");
			//stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT order by MajorId DESC;");
			int res = r.nextInt(3);
			if (res == 0) stmt.executeQuery("SELECT SId, SName from STUDENT;");
			if (res == 1) stmt.executeQuery("SELECT SId, SName, GradYear from STUDENT where GradYear = 2004;");
			if (res == 2) stmt.executeQuery("SELECT SId, SName, MajorId from STUDENT order by MajorId DESC;");
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		serverThread.stop();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
		stmt = mainConnection.createStatement();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
	
	}
}
