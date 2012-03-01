package simpledb.tx;

import java.util.ArrayList;
import java.util.List;

import simpledb.tx.concurrency.LockAbortException;

/**
 * This is a class for Lock checking and processing
 * 
 * @author Yidi CS4432
 * 
 */
public class LockCheck {
	
	private int waitingFor;
	private int lockType;
	private List<Transaction> txList;
	private List<Transaction> XLockWaitingList;
	private List<Transaction> SLockWaitingList;
	

	public LockCheck() {
		
		this.waitingFor = 0;
		this.lockType = 0;
		this.txList = new ArrayList<Transaction>();
		this.XLockWaitingList = new ArrayList<Transaction>();
		this.SLockWaitingList = new ArrayList<Transaction>();
		
	}

	public void sLock(Transaction transaction) {
		if (lockType == -1) {
			if (oldest(transaction)) {
				wound();
				lockType++;
				txList.add(transaction);
			} else {
				transactionWaits(transaction, "S");
			}
		} else {
			lockType++;
			txList.add(transaction);
		}
	}

	public void xLock(Transaction transaction) {
		if (lockType != 0) {
			if (oldest(transaction)) {
				wound();
				lockType = -1;
				txList.add(transaction);
			} else {
				transactionWaits(transaction, "X");
			}
		} else {
			lockType = -1;
			txList.add(transaction);
		}
	}

	
	//Wounds younger(the transactions of current lock)
	private void wound() {
		for (Transaction t : txList) {
			t.rollback();
		}
	}

	
	private void transactionWaits(Transaction transaction, String waitingType) {
		if (waitingType == "X") {
			XLockWaitingList.add(transaction);
		} else if (waitingType == "S") {
			SLockWaitingList.add(transaction);
		}
		waitingFor++;
		try {
			wait();
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	//check whether or not the transaction is the oldest in the txList
	private boolean oldest(Transaction transaction) {
		
		int txNumber = transaction.getTxNum();
		for (Transaction t : txList) {
			
			if (txNumber < t.getTxNum()) 
				return true;
			
		}
		
		return false;
	}


	//unlock transaction
	public synchronized void unLock(Transaction transaction) {
		if (lockType - 1 > 0) {
			lockType = lockType - 1;
			txList.remove(transaction);
		} else {
			lockType = 0;
			txList.remove(transaction);
			findOldestLock();
		}
	}

	//
	private void findOldestLock() {
		
		if (waitingFor > 0) {
			
			String nextLockType = getNextLockType();

			if (nextLockType == "Unknown") {
				
				Transaction oldestX = getOldestTxForXLock();
				Transaction oldestS = getOldestTxForSLock();

				if (oldestX.getTxNum() < oldestS.getTxNum()) {
					nextLockType = "X";
					
				} else {
					
					nextLockType = "S";
					
				}
			}

			//notify oldest when nextLockType is "X"
			if (nextLockType == "X") {
				
				Transaction oldesttransaction = getOldestTxForXLock();
				XLockWaitingList.remove(oldesttransaction);
				oldesttransaction.notify();
				
			} else if (nextLockType == "S") {
				//notify whole list if nextLockType is "S"
				
				for (Transaction t : SLockWaitingList) {
					
					SLockWaitingList.remove(t);
					t.notify();
					
				}
			}
		}
	}

	// you need to discuss three situation: x&s, x, s
	private String getNextLockType() {

		if ((XLockWaitingList.size() > 0) && (SLockWaitingList.size() > 0))

			return "Unknown";

		else if (XLockWaitingList.size() > 0)

			return "X";

		else
			return "S";

	}

	
	private Transaction getOldestTxForSLock() {

		Transaction oldesttransaction = SLockWaitingList.get(0);
		int oldestAge = oldesttransaction.getTxNum();

		for (Transaction t : SLockWaitingList) {

			if (t.getTxNum() <= oldestAge) {

				oldestAge = t.getTxNum();
				oldesttransaction = t;

			}

		}

		return oldesttransaction;
	}

	private Transaction getOldestTxForXLock() {

		Transaction oldesttransaction = XLockWaitingList.get(0);
		int oldestAge = oldesttransaction.getTxNum();

		for (Transaction t : XLockWaitingList) {

			if (t.getTxNum() <= oldestAge) {

				oldestAge = t.getTxNum();
				oldesttransaction = t;

			}
		}

		return oldesttransaction;
	}

}
