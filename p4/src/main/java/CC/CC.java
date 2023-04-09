package CC;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class CC
{
	/**
	 * Inner class representing the LockTable.
	 */
	private static class LockTable {
		private Map<Integer, Map<Integer, LockType>> table;    // <RecordId, <>>

		public LockTable(int recordCount) {    // new LockTable constructor
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
		//TODO
		return null ;
	}
}
