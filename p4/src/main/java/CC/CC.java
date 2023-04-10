package CC;
import java.util.*;

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
		int[] timeTransaction = new int[numTrans];

		// Insert actions
		for (int i = 0; i < numTrans; i++) {
			action[i] = new LinkedList<>();
			String[] operations = transactions.get(i).split(";");
			for (String token:operations) {
				action[i].add(token);
			}
			timeTransaction[i] = -1;
		}

		// Round-Robin execution of actions from transactions
		do{
			noMoreToken = 0;
			for (int i = 0; i < numTrans; i++) {
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
//							System.out.printf("W(%d, %d)\n", recordId, value);
							log.add(String.format("W:%d,T%d,%d,%d,%d,%d",timestamp, transactionId, recordId, oldValue, value, timeTransaction[i]));
							timeTransaction[i] = timestamp;
							timestamp++;
						}

					} else if (token.startsWith("R")) { // R(<RecordID>)
						int recordId = Integer.parseInt(token.substring(2, token.length() - 1));
						boolean locked = lockTable.acquireLock(recordId, transactionId, LockType.SHARED);
						if (locked) {
							int readValue = db[recordId];
							action[i].remove();
//							System.out.printf("R(%d)\n", recordId);
							log.add(String.format("R:%d,T%d,%d,%d,%d",timestamp, transactionId, recordId, readValue, timeTransaction[i]));
							timeTransaction[i] = timestamp;
							timestamp++;
						}
					} else if (token.equals("C")) { // C
						lockTable.releaseLocks(transactionId);
						action[i].remove();
//						System.out.println("C");
						log.add(String.format("C:%d,T%d,%d",timestamp, transactionId, timeTransaction[i]));
						timeTransaction[i] = timestamp;
						timestamp++;
					}
					/* -------TOKEN PROCESS------- */
				} else {
					noMoreToken++;
				}
			}
		} while (noMoreToken < numTrans);

		// print log to console
		for (String str:log) {
			System.out.println(str);
		}

		return db;
	}
}
