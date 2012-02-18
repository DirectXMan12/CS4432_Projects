package com.wpi.cs4432.simpledb.tests.performance;

import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;

public class IndexPerfTests2 extends SimpleDBBaseTest
{
	public static int randVal;

	@BeforeClass
	public static void initRandConst()
	{
		Random r = new Random();
		randVal = r.nextInt(1000);
	}
	
	@Test
	public void mainTest() throws SQLException
	{
		long start, sel_sh, sel_none, sel_eh, sel_bt, join_sh, join_none, join_bt, join_eh = 0;
		start = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a11, a12 from test1 where a11 = "+randVal+";");
		sel_sh = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a41, a42 from test4 where a41 = "+randVal+";");
		sel_none = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a31, a32 from test3 where a31 = "+randVal+";");
		sel_bt = new Date().getTime();
		
		sel_eh = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a41, a42, a51, a52 from test4, test5 where a41 = a51;");
		join_none = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a11, a12, a51, a52 from test1, test5 where a11 = a51;");
		join_sh = new Date().getTime();
		
		join_eh = new Date().getTime();
		
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a31, a32, a51, a52 from test3, test5 where a31 = a51;");
		join_bt = new Date().getTime();

		long[] sel_times = new long[] { sel_sh-start, sel_none-sel_sh, sel_bt-sel_none, sel_eh-sel_bt };
		long[] join_times = new long[] { join_none - sel_eh, join_sh-join_none, join_eh-join_sh, join_bt-join_eh};
		String[] sel_labels = new String[] { "sh", "none", "bt", "eh" };
		String[] join_labels = new String[] { "none", "sh", "eh", "bt" };
		
		System.out.println(jsonify(sel_times, sel_labels, join_times, join_labels));
	}

	protected String jsonify(long[] sel_times, String[] sel_labels, long[] join_times, String[] join_labels)
	{
		StringBuilder sb = new StringBuilder("{ \"sel\": { ");
		
		for (int i = 0; i < sel_times.length; i++)
		{
			sb.append("\"");
			sb.append(sel_labels[i]);
			sb.append("\": ");
			sb.append(sel_times[i]);
			if (i < sel_times.length-1) sb.append(", ");
		}
		
		sb.append(" }, \"join\": {");
		for (int i = 0; i < join_times.length; i++)
		{
			sb.append("\"");
			sb.append(join_labels[i]);
			sb.append("\": ");
			sb.append(join_times[i]);
			if (i < join_times.length-1) sb.append(", ");
		}
		
		sb.append("} }");
		return sb.toString();
	}
}
