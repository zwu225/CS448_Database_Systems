package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class HashJoin extends Iterator {

	private int leftCol;
	private int rightCol;
	private IndexScan leftScan;
	private IndexScan rightScan;
	private HashTableDup leftHashTable = null;
	private Queue<Tuple> resultTuples = new ArrayDeque<>();
	private Tuple lastLeftTuple = null;		// head of new bucket
	private int lastLeftBucket = -1;
	private Tuple lastRightTuple = null;		// head of new bucket
	private int lastRightBucket = -1;

	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		this.leftCol = aJoinCol1;
		this.rightCol = aJoinCol2;
//		this.getNext = false;
		// init left IndexScan
		if (aIter1 instanceof IndexScan) {
			this.leftScan = (IndexScan) aIter1;
		} else {
			Schema leftSchema = aIter1.getSchema();
			HashIndex leftIndex = new HashIndex(null);
			HeapFile leftHeapFile = new HeapFile(null);
			while(aIter1.hasNext()) {
				Tuple tempT = aIter1.getNext();
				leftIndex.insertEntry(
						new SearchKey(tempT.getField(leftCol)),
						tempT.insertIntoFile(leftHeapFile)
				);
			}
			this.leftScan = new IndexScan(leftSchema, leftIndex, leftHeapFile);
//			this.leftScan.restart();
		}
		// init right IndexScan
		if (aIter2 instanceof IndexScan) {
			this.rightScan = (IndexScan) aIter2;
		} else {
			Schema rightSchema = aIter2.getSchema();
			HashIndex rightIndex = new HashIndex(null);
			HeapFile rightHeapFile = new HeapFile(null);
			while(aIter2.hasNext()) {
				Tuple tempT = aIter2.getNext();
				rightIndex.insertEntry(
						new SearchKey(tempT.getField(rightCol)),
						tempT.insertIntoFile(rightHeapFile)
				);
			}
			this.rightScan = new IndexScan(rightSchema, rightIndex, rightHeapFile);
//			this.rightScan.restart();
		}

		// close the Iterators
		aIter1.close();
		aIter2.close();
		// join the schema
		this.schema = Schema.join(leftScan.schema, rightScan.schema);
	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		leftScan.restart();
		rightScan.restart();
	}

	@Override
	public boolean isOpen() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		return leftScan.isOpen() && rightScan.isOpen();
	}

	@Override
	public void close() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		leftScan.close();
		rightScan.close();
	}

	@Override
	public boolean hasNext() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		// resultTuples still has matched items
		if (!resultTuples.isEmpty()) {
			return true;
		}

		/** LEFT ITERATOR BUCKET SWEEP **/
		// first iteration, fill in the head of the left bucket
		if (lastLeftTuple == null) {
			if (leftScan.hasNext()) {
				lastLeftTuple = leftScan.getNext();
				lastLeftBucket = leftScan.getNextHash();
			} else {
				return false;	// no more tuple in left
			}
		}

		leftHashTable = new HashTableDup();	// init new left hash table
		leftHashTable.add(new SearchKey(lastLeftTuple.getField(leftCol)), lastLeftTuple);//add bucket head to hash table

		Tuple leftTemp;
		int leftTempBucket;
		int currentLeftBucket = lastLeftBucket;

		while (true) {
			if (leftScan.hasNext()) {
				leftTemp = leftScan.getNext();
				leftTempBucket = leftScan.getNextHash();
				if (leftTempBucket == lastLeftBucket) { // same bucket as the head
					leftHashTable.add(new SearchKey(leftTemp.getField(leftCol)),leftTemp); // add to the hash
				} else { // different key from the head, remake it to new head
					lastLeftTuple = leftTemp;
					lastLeftBucket = leftTempBucket;
					break; // that is all for the current key
				}
			} else {	//no more tuple beyond the head
				break;
			}
		} //end while, leftHashTable is set, lastLeftTuple updated

		/** RIGHT ITERATOR BUCKET SWEEP **/

		// first iteration, fill in the head of the right bucket
		if (lastRightTuple == null) {
			if (rightScan.hasNext()) {
				lastRightTuple = rightScan.getNext();
				lastRightBucket = rightScan.getNextHash();
			} else {
				return false;	// no more tuple in right
			}
		}
//		int currentRightBucket = lastRightBucket;

		// process right iter, one by one until a different key
		while (true) {
			if (lastRightBucket == currentLeftBucket) { // left and right in same bucket
				Tuple[] sameKeyTuples = leftHashTable.getAll(new SearchKey(lastRightTuple.getField(rightCol)));
				for (Tuple leftJoinTemp : sameKeyTuples) {
					resultTuples.add(Tuple.join(leftJoinTemp, lastRightTuple, this.schema));
				}
				// advance the right last tuple and bucket number
				lastRightTuple = rightScan.getNext();
				lastRightBucket = rightScan.getNextHash();
			} else { // right is in next bucket
				break;
			}
		}
		return true;
	}

	@Override
	public Tuple getNext() {
//		throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if (resultTuples.isEmpty()) {
			if (!hasNext()) {	// no more tuple
				throw new IllegalStateException("ERROR: Hashjoin no more tuples!");
			}
		}
//		System.out.println(resultTuples.isEmpty());
		return resultTuples.remove();
	}
} // end class HashJoin;
