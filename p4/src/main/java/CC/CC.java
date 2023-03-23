package CC;
import java.util.List;

public class CC
{

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
	public static int[] executeSchedule(int[] db, List<String> transactions)
	{
		//TODO
		return null ;
	}
}
