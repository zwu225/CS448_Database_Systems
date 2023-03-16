package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
	
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
	  throw new UnsupportedOperationException("Not implemented");
    //TODO: Your code here
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  throw new UnsupportedOperationException("Not implemented");

    //TODO: Your code here
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  throw new UnsupportedOperationException("Not implemented");
    //TODO: Your code here
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  throw new UnsupportedOperationException("Not implemented");
    //TODO: Your code here
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  throw new UnsupportedOperationException("Not implemented");
    //TODO: Your code here
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  throw new UnsupportedOperationException("Not implemented");
    //TODO: Your code here
  }

} // public class KeyScan extends Iterator
