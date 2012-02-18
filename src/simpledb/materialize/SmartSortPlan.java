/**
 * 
 */
package simpledb.materialize;

import java.util.List;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * @author directxman12
 *
 */
public class SmartSortPlan extends SortPlan
{

	protected TableInfo tblInfo;
	protected RecordFile _rf;
	
	public SmartSortPlan(Plan p, List<String> sortfields, Transaction tx, String tblname)
	{
		super(p, sortfields, tx);
		tblInfo = SimpleDB.mdMgr().getTableInfo(tblname, tx);
		_rf = new RecordFile(tblInfo, tx);
	}
	
	@Override
	public Scan open()
	{
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src);
		src.close();
		while (runs.size() > 2)
			runs = doAMergeIteration(runs);
		return new SmartSortScan(runs, comp, _rf, tx);
	}
	
	

}
