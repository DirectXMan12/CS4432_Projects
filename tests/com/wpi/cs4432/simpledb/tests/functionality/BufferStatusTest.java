package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.*;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.LRUBasicBufferMgr;
import simpledb.buffer.MRUBasicBufferMgr;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.studentClient.CreateStudentDB;

public class BufferStatusTest {
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
	
	@Before
	public void setupBefore() throws SQLException
	{
		if (stmt == null) stmt = mainConnection.createStatement();
		stmt.executeUpdate("delete from STUDENT where SId = 10;");
	}
	
	public String[][] getResultSetAsArray(ResultSet rs, String cols[], Class cls[]) throws SQLException
	{
		LinkedList<String[]> res = new LinkedList<String[]>();
		
		while(rs.next())
		{
			String[] row = new String[cols.length];
			for (int i = 0; i < cols.length; i++)
			{
				if(cls[i] == String.class) row[i] = rs.getString(cols[i]);
				else if(cls[i] == Integer.class) row[i] = Integer.toString(rs.getInt(cols[i]));
				else throw new RuntimeException("Ohes noze!");
			}
			res.add(row);
		}
		
		return res.toArray(new String[][] {});
	}
	
	public String stringArrayToString(String[][] strs)
	{
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < strs.length; i++)
		{
			sb.append('[');
			for (int j = 0; j < strs[i].length; j++)
			{
				sb.append(strs[i][j]);
				if (j < strs[i].length - 1) sb.append(",");
			}
			sb.append(']');
			if (i < strs.length - 1) sb.append(',');
		}
		sb.append(']');
		
		return sb.toString();
	}
	
	@Test
	public void testBufferStatus() throws SQLException
	{
		BufferMgr buffMgr = SimpleDB.bufferMgr();
		System.out.println(buffMgr.toString());
		stmt.executeQuery("select sid, sname from student;");
		System.out.println(buffMgr.toString());
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		serverThread.stop();
	}
}