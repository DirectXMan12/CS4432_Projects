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
 * An extensible hash index. Uses the Index interface.
 * The number of buckets used to store the data varies depending on
 * how much space is needed. The initial number of buckets is 4
 * and each bucket is implemented as a file of index records.
 * @author Brian and Solly (mostly Solly)
 * {@inheritDoc}
 */
public class ExtHashIndex implements Index
{
	public static int BUCKET_SIZE;		// number of records per bucket
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
		
		dir_sch = new Schema();
		dir_sch.addStringField("bnum", Integer.SIZE*8);
		dir_sch.addIntField("recnum");
		dir_sch.addIntField("res");
		dir_ti = new TableInfo(idxname+"dir", dir_sch);
		dir_ts = new TableScan(ti, tx);
		
		index_resolution = 1; 
		NUM_BUCKETS = (int) Math.pow(2, index_resolution);
		bucket_map = new String[NUM_BUCKETS];
		int currMax = index_resolution;
		dir_ts.beforeFirst();
		while(dir_ts.next()) if (currMax < dir_ts.getInt("res")) currMax = dir_ts.getInt("res");
		index_resolution = currMax;
	}
	
	/**
	 * finds the actual key of a bucket for use with the bucket map
	 * @param skey
	 * @return the key based on the acual bucket page
	 */
	protected String calc_act_key(Constant skey)
	{
		return bucket_map[skey.hashCode() % NUM_BUCKETS];
	}
	
	/**
	 * Finds the virtual key for use with the bucket map
	 * @param virt_key
	 * @return the virtual key for the bucket page
	 */
	protected String get_act_key(int virt_key)
	{
		return bucket_map[virt_key];
	}
	
	/**
	 * determines what the virtual key is
	 * @param skey
	 * @return an int representing the virtual key
	 */
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
	
	/**
	 * indicates if a bucket is full or not
	 * @param bname
	 * @return true if the bucket is full and false if it still has space
	 */
	protected boolean bucketIsFull(String bname)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(bname)) break;
		return dir_ts.getInt("recnum")*RECORD_LEN >= BLOCK_SIZE; 
	}
	
	/**
	 * Used to increment through the index bucket by bucket
	 * @param by_num
	 * @param act_bucket_num
	 */
	protected void incrementBucketRecords(int by_num, String act_bucket_num)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(act_bucket_num)) break;
		dir_ts.setInt("recnum", dir_ts.getInt("recnum")+by_num);
	}
	
	/**
	 * Stores the values in a bucket
	 * @param num
	 * @param act_bucket_num
	 */
	protected void setBucketRecords(int num, String act_bucket_num)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(act_bucket_num)) break;
		dir_ts.setInt("recnum", num);
	}
	
	/**
	 * called when the number of buckets needs to be
	 * increased to provide space for more records
	 */
	public void expand()
	{
		//NUM_BUCKETS = (2 * NUM_BUCKETS); // Brian's Attempt - Failed
		increase_resolution();
		beforeFirst(searchkey);
	}

	/**
	 * Increases the resolution of the buckets. 
	 * Once the resolution of the buckets has been
	 * increased, it allows for more records to
	 * be stored.
	 */
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
	
	/**
	 * Used to find the identifier for a bucket
	 * @param virt_key_spot
	 * @return the identifier of a bucket
	 */
	protected String getTblSuffix(int virt_key_spot)
	{
		return String.format("%0"+index_resolution+"d", virt_key_spot);
	}
	
	/**
	 * Is used to determine if the bucket resolution can be increase
	 * @param act_key
	 * @return true if the bucket is at max resolution or false otherwise
	 */
	protected boolean bucketIsMaxRes(String act_key)
	{
		dir_ts.beforeFirst();
		while(dir_ts.next()) if(dir_ts.getString("bnum").equals(act_key)) break;
		return dir_ts.getInt("res") >= index_resolution;
	}

	/**
	 * {@inheritDoc}
	 */
	public void insert(Constant ext_dataval, RID ext_datarid)
	{
		if(bucketIsFull(calc_act_key(ext_dataval)))
		{
			int old_virt_key = calc_virt_key(ext_dataval);
			if (!bucketIsMaxRes(calc_act_key(ext_dataval)))
			{
				bucket_map[old_virt_key+1] = getTblSuffix(old_virt_key+1);
				recalc_buckets(old_virt_key);
			}
			else
			{
				expand();
				bucket_map[2*old_virt_key+1] = getTblSuffix(2*old_virt_key+1);
				recalc_buckets(2*old_virt_key);
			}
		}
		beforeFirst(ext_dataval);
		
		ts.insert();
		ts.setInt("block", ext_datarid.blockNumber());
		ts.setInt("id", ext_datarid.id());
		ts.setVal("dataval", ext_dataval);
		incrementBucketRecords(1, calc_act_key(ext_dataval));
	}

	/**
	 * Recalculates the keys that show where buckets
	 * are in the index
	 * @param virt_key_spot
	 */
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
	
	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb)
	{
		return numblocks*rpb / BUCKET_SIZE + 1;
	}
}
