package simpledb.materialize;

import java.util.List;

import simpledb.query.Scan;
import simpledb.query.UpdateScan;
//import simpledb.record.RID;
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

public class SmartSortScan extends AbstractSortScan
{
	protected RecordFile _rf;
	protected TempTable _tmpTable;
	protected UpdateScan _scn;
	protected Schema _schema;
	
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
		_rf.setSorted(true);
		super.close();
	}
}
