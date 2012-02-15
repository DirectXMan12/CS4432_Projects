package simpledb.materialize;

import simpledb.query.*;
import java.util.*;

/**
 * The Smart Scan class for the <i>sort</i> operator.
 * @author Jeff Namias
 */
/**
 * @author namias
 *
 */

public class SmartSortScan extends AbstractSortScan implements Scan {
	   /**
	    * {@inheritDoc}
	    */
	   public SmartSortScan(List<TempTable> runs, RecordComparator comp) {
	      super(runs,comp);
	   }
}
