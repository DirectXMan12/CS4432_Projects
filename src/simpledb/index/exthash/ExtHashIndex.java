package simpledb.index.exthash;

import java.net.IDN;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.derby.iapi.services.io.ArrayUtil;

import simpledb.tx.Transaction;
import static simpledb.file.Page.BLOCK_SIZE;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.file.Block;
import simpledb.index.Index;

/**
 * @author MARCO
 * {@inheritDoc}
 */
public class ExtHashIndex implements Index
{
	public int BUCKET_SIZE;		// number of records per bucket
	public int NUM_BUCKETS;
	protected int RECORD_LEN;
	public int index_resolution;
	protected String idxname;
	protected Schema sch;
	protected Transaction tx;
	protected Constant searchkey = null;
	protected TableScan ts = null;
	String[] bucket_map;
	
	protected Schema dir_sch;
	protected TableInfo dir_ti;
	protected TableScan dir_ts;
	
	/**
	 * {@inheritDoc}
	 */
	public ExtHashIndex(String ext_idxname, Schema ext_sch, Transaction ext_tx)
	{
		this.idxname = ext_idxname;
		this.sch = ext_sch;
		this.tx = ext_tx;
		
		TableInfo ti = new TableInfo(idxname+"0", sch);
		RECORD_LEN = ti.recordLength();
		BUCKET_SIZE = BLOCK_SIZE/RECORD_LEN;
		
		index_resolution = 1; // TODO: load resolution num buckets
		NUM_BUCKETS = (int) Math.pow(2, index_resolution);
		bucket_map = new String[NUM_BUCKETS];
		
		dir_sch = new Schema();
		dir_sch.addStringField("bnum", Integer.SIZE*8);
		dir_sch.addIntField("recnum");
		dir_ti = new TableInfo(idxname+"dir", dir_sch);
		dir_ts = new TableScan(ti, tx);
	}
	
	protected String calc_act_key(Constant skey)
	{
		return bucket_map[skey.hashCode() % NUM_BUCKETS];
	}
	
	protected String get_act_key(int virt_key)
	{
		return bucket_map[virt_key];
	}
	
	protected int calc_virt_key(Constant skey)
	{
		return skey.hashCode() % NUM_BUCKETS;
	}

	/**
	 * {@inheritDoc}
	 */
	public void beforeFirst(Constant ext_searchkey)
	{
		close();
		this.searchkey = ext_searchkey;
		int calc_bucket = ext_searchkey.hashCode() % NUM_BUCKETS;
		String act_bucket = bucket_map[calc_bucket];
		String tblname = idxname + act_bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean next()
	{
		while (ts.next())
			if(ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public RID getDataRid()
	{
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}
	
	protected boolean bucketIsFull(String bname)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(bname)) break;
		return dir_ts.getInt("recnum")*RECORD_LEN >= BLOCK_SIZE; 
	}
	
	protected void incrementBucketRecords(int by_num, String act_bucket_num)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(act_bucket_num)) break;
		dir_ts.setInt("recnum", dir_ts.getInt("recnum")+by_num);
	}
	
	protected void setBucketRecords(int num, String act_bucket_num)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(act_bucket_num)) break;
		dir_ts.setInt("recnum", num);
	}
	
	/**
	 * 
	 */
	public void expand()
	{
		//NUM_BUCKETS = (2 * NUM_BUCKETS);
		increase_resolution();
		beforeFirst(searchkey);
	}

	protected void increase_resolution()
	{
		index_resolution++;
		NUM_BUCKETS = (int) Math.pow(2, index_resolution);
		String[] new_bucket_map = new String[NUM_BUCKETS];
		for (int i = 0; i < new_bucket_map.length/2; i++)
		{
			new_bucket_map[i*2] = bucket_map[i];
			new_bucket_map[i*2+1] = bucket_map[i];
		}
		bucket_map = new_bucket_map;
	}
	
	protected String getTblSuffix(int virt_key_spot)
	{
		return String.format("%0"+index_resolution+"d", virt_key_spot);
	}

	/**
	 * {@inheritDoc}
	 */
	public void insert(Constant ext_dataval, RID ext_datarid)
	{
		if(bucketIsFull(calc_act_key(ext_dataval)))
		{
			int old_virt_key = calc_virt_key(ext_dataval);
			expand();
			int virt_key_spot = calc_virt_key(ext_dataval);
			bucket_map[2*old_virt_key+1] = getTblSuffix(virt_key_spot);
			recalc_buckets(virt_key_spot);
		}
		beforeFirst(ext_dataval);
		
		ts.insert();
		ts.setInt("block", ext_datarid.blockNumber());
		ts.setInt("id", ext_datarid.id());
		ts.setVal("dataval", ext_dataval);
		incrementBucketRecords(1, calc_act_key(ext_dataval));
		
	}

	protected void recalc_buckets(int virt_key_spot)
	{
		TableInfo tmp_ti = new TableInfo(idxname+get_act_key(virt_key_spot), sch);
		TableScan tmp_ts = new TableScan(tmp_ti, tx);
		TableInfo tmp_ti2 = new TableInfo(idxname+get_act_key(virt_key_spot+1), sch);
		TableScan tmp_ts2 = new TableScan(tmp_ti, tx);
		tmp_ts.beforeFirst();
		tmp_ts2.beforeFirst();
		
		int rec1 = 0;
		int rec2 = 0;
		
		while (tmp_ts.next())
		{
			int new_bucket = calc_virt_key(tmp_ts.getVal("dataval"));
			if (new_bucket == virt_key_spot) rec1++;
			else if (new_bucket == virt_key_spot+1)
			{
				tmp_ts2.insert();
				tmp_ts2.setInt("block", tmp_ts.getInt("block"));
				tmp_ts2.setInt("id", tmp_ts.getInt("id"));
				tmp_ts2.setVal("dataval", tmp_ts.getVal("dataval"));
				
				tmp_ts.delete();
			}
			else throw new RuntimeException("Gadzooks!");
		}
		
		tmp_ts2.beforeFirst();
		tmp_ts.beforeFirst();
		
		while (tmp_ts2.next())
		{
			int new_bucket = calc_virt_key(tmp_ts2.getVal("dataval"));
			if (new_bucket == virt_key_spot+1) rec2++;
			else if (new_bucket == virt_key_spot)
			{
				tmp_ts.insert();
				tmp_ts.setInt("block", tmp_ts2.getInt("block"));
				tmp_ts.setInt("id", tmp_ts2.getInt("id"));
				tmp_ts.setVal("dataval", tmp_ts2.getVal("dataval"));
				
				tmp_ts2.delete();
			}
			else throw new RuntimeException("Gadzooks!");
		}
		
		tmp_ts.close();
		tmp_ts2.close();
		
		setBucketRecords(rec1, get_act_key(virt_key_spot));
		setBucketRecords(rec2, get_act_key(virt_key_spot+1));
	}

	/**
	 * {@inheritDoc}
	 */
	public void delete(Constant ext_val,RID ext_rid)
	{
		beforeFirst(ext_val);
		while(next())
		{
			if(getDataRid().equals(ext_rid))
			{
				ts.delete();
				return;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public void close()
	{
		if (ts != null) ts.close();		
	}

}
