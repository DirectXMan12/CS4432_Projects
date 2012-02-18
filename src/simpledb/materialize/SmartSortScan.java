package simpledb.materialize;

import java.util.List;

import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Smart Scan class for the <i>sort</i> operator.
 * @author Jeff Namias
 */
/**
 * @author namias
 *
 */

public class SmartSortScan extends AbstractSortScan implements Scan
{
	protected RecordFile _rf;
	protected TempTable _tmpTable;
	protected UpdateScan _scn;
	protected Schema _schema;
	
	// TODO: step 1: find Delorean
	// 		 step 2: go back in time and "deal with" author
	// 		 step 3: write a coherent, modular database system for him
	// 		 step 4: return to present
	// 		 step 5: actually do database programming
	   /**
	* {@inheritDoc}
	*/
	public SmartSortScan(List<TempTable> runs, RecordComparator comp, RecordFile rf, Transaction tx)
	{
		super(runs,comp);
		_rf = rf;
		_schema = runs.get(0).getTableInfo().schema();
		_tmpTable = new TempTable(_schema, tx);
		_scn = _tmpTable.open();
	}
	   
	@Override
	public boolean next()
	{
		boolean res = super.next();
		return res;
	}
	
	protected void copyRecord()
	{
		if (!_scn.next()) _scn.insert();
		for (String colName : _schema.fields())
		{
			_scn.setVal(colName, super.getVal(colName));
		}
	}
	
	@Override
	public void beforeFirst()
	{
		super.beforeFirst();
		_scn.beforeFirst();
	}

	@Override
	public void close()
	{
		while(this.next()) { };
		_scn.beforeFirst();
		_rf.beforeFirst();
		while(_scn.next())
		{
			for (String colName : _schema.fields())
			{
				
				Object v = _scn.getVal(colName).asJavaVal();
				if (v instanceof Number) _rf.setInt(colName, ((Number)v).intValue());
				else _rf.setString(colName, v.toString());
			}
		}
		super.close();
	}
}
