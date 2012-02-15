package simpledb.materialize;

//import simpledb.record.RID;
import simpledb.query.*;
import java.util.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 * @author Jeff Namias
 */
/**
 * @author sciore
 * @author namias
 *
 */
public class SortScan extends AbstractSortScan implements Scan {

   
   /**
    * {@inheritDoc}
    */
   public SortScan(List<TempTable> runs, RecordComparator comp) {
      super(runs,comp);
   }
   
   
}
