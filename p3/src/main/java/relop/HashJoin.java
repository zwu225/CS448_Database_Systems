package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

public class HashJoin extends Iterator {
	
	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public boolean isOpen() {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public boolean hasNext() {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public Tuple getNext() {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}
} // end class HashJoin;
