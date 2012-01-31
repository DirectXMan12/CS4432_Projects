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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
		Statement stmt = mainConnection.createStatement();
		stmt.executeQuery("select SId, SName, MajorId, GradYear from STUDENT");
		
	}
	
	@Test
	public void testStudentDBSelectPerf() throws SQLException
	{
		Statement stmt = mainConnection.createStatement();
		for(int i = 1; i < 1000; i++)
		{
			stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT;");
			stmt.executeQuery("SELECT SId, SName, MajorId, GradYear from STUDENT;");
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
	
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
	
	}
}
