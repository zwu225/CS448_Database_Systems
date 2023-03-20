package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

    private HeapFile file = null;
    private HashIndex index = null;
    private HashScan scan = null;
    private SearchKey key = null;
    private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
    // Your code here
      this.schema = aSchema;
      this.file = aFile;
      this.index = aIndex;
      this.key = aKey;
      this.scan = index.openScan(key);
      isOpen = true;
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
    // Your code here
      scan.close();
      scan = index.openScan(key);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    // Your code here
      return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    // Your code here
      scan.close();
      isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // Your code here
      return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // Your code here
      return new Tuple(schema, file.selectRecord(scan.getNext()));
  }

} // public class KeyScan extends Iterator
