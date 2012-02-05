/**
 * 
 */
package simpledb.application;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.remote.ConnectionAdapter;
import simpledb.remote.DriverAdapter;
import simpledb.remote.SimpleDriver;

/**
 * @author Jeff
 *
 */
public class SimpleDBConnect {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost"; //my ip
		//String host = "csta096.cs.wpi.edu";
		String url = "jdbc:simpledb://" + host;
		String qry ="Create table test1" + 
					"( a1 int," +
					" a2 int"+
					");";
		Statement s=null;
		try {
			conn = d.connect(url, null);
			s=conn.createStatement();
			System.out.println("Create table: "+s.executeUpdate(qry));
			qry="insert into test1(a1, a2) values (432, 957);";
			System.out.println("Insert table: "+s.executeUpdate(qry));
			qry="select a1,a2 from test1;";
			ResultSet rs=s.executeQuery(qry);
			while(rs.next())
			{
				System.out.println(qry+"\n Query result: "+"a1="+rs.getInt("a1")+ " a2="+rs.getInt("a2"));
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(conn.toString());
	}

}
