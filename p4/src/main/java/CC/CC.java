package CC;
import java.util.*;

public class CC
{
	/**
	 * Inner class representing the LockTable.
	 */
	private static class LockTable {
		private Map<Integer, Map<Integer, LockType>> table;    // <RecordId, <>>
		private int numRecords;

		public LockTable(int recordCount) {    // new LockTable constructor
			this.numRecords = recordCount;
			table = new HashMap<>();
			for (int i = 0; i < recordCount; i++) {
				table.put(i, new HashMap<>());
			}
		}

		public boolean acquireLock(int recordId, int transactionId, LockType lockType) {
			Map<Integer, LockType> locks = table.get(recordId);
			if (locks.isEmpty()) { // no locks
				locks.put(transactionId, lockType);
				return true;
			} else { // contains locks
				if (locks.containsKey(transactionId)) { // current transaction has lock
					LockType currentLockType = locks.get(transactionId);
					if (lockType == LockType.SHARED) {
						if (currentLockType == LockType.EXCLUSIVE) {
							return true;
						} else if (currentLockType == LockType.SHARED) {
							return true;
						}
					} else if (lockType == LockType.EXCLUSIVE && currentLockType == LockType.EXCLUSIVE) { // current T already has EXCLUSIVE lock
						return true;
					} else if (lockType == LockType.EXCLUSIVE && currentLockType == LockType.SHARED && locks.size() == 1) {
						return true;
					}
				} else { // current transaction don't have lock, so other transactions have locks
					if (lockType == LockType.SHARED) {
						if (!locks.containsValue(LockType.EXCLUSIVE)) {
							locks.put(transactionId, lockType);
							return true;
						}
					} else if (lockType == LockType.EXCLUSIVE) { // cannot obtain lock because other locks on other Ts
						return false;
					}
				}
			}
			return false;
		}

		public void releaseLocks(int transactionId) {
			for (Map<Integer, LockType> locks : table.values()) {
				if (locks.containsKey(transactionId)) {
					locks.remove(transactionId);
				}
			}
		}

		public boolean holdsLock(int recordId, int transactionId) {
			Map<Integer, LockType> locks = table.get(recordId);
			return locks.containsKey(transactionId);
		}

		public int abortTrans() {
			for (int i = numRecords; i > 0; i--) {
				int unlockCount = 0;
				for (Map<Integer, LockType> locks : table.values()) { // check for locks in current transaction
					if (locks.containsKey(i)) {    //remove all the locks from the lowest priority transaction
						locks.remove(i);
						unlockCount++;
					}
				}
				if (unlockCount > 0) {    // if any unlocked, do not continue to the next transaction
					return i;
				}
			}
			return -1; // return -1: no active locks
		}
	}

	public enum LockType {
		NONE, SHARED, EXCLUSIVE
	}


	/**
	 * Notes:
	 *  - Execute all given transactions, using locking.
	 *  - Each element in the transactions List represents all operations performed by one transaction, in order.
	 *  - No operation in a transaction can be executed out of order, but operations may be interleaved with other
	 *    transactions if allowed by the locking.
	 *  - The index of the transaction in the list is equivalent to the transaction ID.
	 *  - Print the log to the console at the end of the method.
	 *  - Return the new db state after executing the transactions.
	 * @param db the initial status of the db
	 * @param transactions the schedule, which basically is a {@link List} of transactions.
	 * @return the final status of the db
	 */
	public static int[] executeSchedule(int[] db, List<String> transactions) {
		LockTable lockTable = new LockTable(db.length);
		int numTrans = transactions.size();
		Queue<String>[] action = new Queue[numTrans];
		int noMoreToken;
		List<String> log = new ArrayList<>();
		int timestamp = 0;
		int[] timeTransaction = new int[numTrans]; // note: index is (transactionID-1)
		List<Boolean> enable = new ArrayList<>(); // note: T1 is index 0
		int waitCount = 0; // count the number of times any transaction wait for locks (for deadlock control)

		// Insert actions
		for (int i = 0; i < numTrans; i++) {
			action[i] = new LinkedList<>();
			String[] operations = transactions.get(i).split(";");
			for (String token:operations) {
				action[i].add(token);
			}
			timeTransaction[i] = -1;
		}

		// init enable-abort flag for all transactions (for deadlock control)
		for (int i = 0; i < numTrans; i++) {
			enable.add(true);    // init as enable for all transactions
		}


		// Round-Robin execution of actions from transactions
		do{
			noMoreToken = 0;
			for (int i = 0; i < numTrans; i++) {
				// skip aborted/finished transaction
				if (!enable.get(i)) { // if current transaction is aborted/finished
					noMoreToken++;
					continue;
				}

				if (!action[i].isEmpty()) {
					String token = action[i].peek();
					int transactionId = i + 1;
					/* -------TOKEN PROCESS------- */
					if (token.startsWith("W")) { // W(<RecordID>,<Value>)
						String[] parts = token.substring(2, token.length() - 1).split(",");
						int recordId = Integer.parseInt(parts[0]);
						int value = Integer.parseInt(parts[1]);
						boolean locked = lockTable.acquireLock(recordId, transactionId, LockType.EXCLUSIVE);
						if (locked) {
							int oldValue = db[recordId];
							db[recordId] = value;
							action[i].remove();
							log.add(String.format("W:%d,T%d,%d,%d,%d,%d",timestamp, transactionId, recordId, oldValue, value, timeTransaction[i]));
							timeTransaction[i] = timestamp;
							timestamp++;
							waitCount = 0;
						} else {
							waitCount++; // count this wait time
						}

					} else if (token.startsWith("R")) { // R(<RecordID>)
						int recordId = Integer.parseInt(token.substring(2, token.length() - 1));
						boolean locked = lockTable.acquireLock(recordId, transactionId, LockType.SHARED);
						if (locked) {
							int readValue = db[recordId];
							action[i].remove();
							log.add(String.format("R:%d,T%d,%d,%d,%d",timestamp, transactionId, recordId, readValue, timeTransaction[i]));
							timeTransaction[i] = timestamp;
							timestamp++;
							waitCount = 0;
						} else {
							waitCount++; // count this wait time
						}
					} else if (token.equals("C")) { // C
						lockTable.releaseLocks(transactionId);
						action[i].remove();
						log.add(String.format("C:%d,T%d,%d",timestamp, transactionId, timeTransaction[i]));
						timeTransaction[i] = timestamp;
						timestamp++;
					}
					/* -------TOKEN PROCESS------- */
				} else { // no more token in current transaction
					enable.set(i, false); // set enable to false
					noMoreToken++;
				}
			} // end for

			/* ---- Deadlock Control ---- */
			// check if deadlock exist
			int activeCount = 0;
			for (Boolean value : enable) { // get the number of trues in enable
				if (value) {
					activeCount++;
				}
			}
			// start abort the lowest priority transaction
			if (waitCount >= activeCount && waitCount > 0) {
				int abortedTransactionId = lockTable.abortTrans();
				enable.set(abortedTransactionId-1, false);
				// roll back
				String abortedTransaction = String.format("T%d",abortedTransactionId);
				for (int i = log.size() - 1; i >= 0; i--) { // roll back using the log
					String str = log.get(i);
					if (str.contains(abortedTransaction) && str.contains("W")) {
						String[] parts = str.split(",");
						int recordId = Integer.parseInt(parts[2]);
						int oldValue = Integer.parseInt(parts[3]);
						db[recordId] = oldValue;
					}
				}
				// add to the log
				log.add(String.format("A:%d,T%d,%d",timestamp, abortedTransactionId, timeTransaction[abortedTransactionId-1]));
				timeTransaction[abortedTransactionId-1] = timestamp;
				timestamp++;
			}

		} while (noMoreToken < numTrans);

		// print log to console
		for (String str:log) {
			System.out.println(str);
		}

		return db;
	}
}
