package relop;

import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import index.HashScan;
import global.RID;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

    private HeapFile file = null;
    private HeapScan heapScan = null;
    private HashIndex index = null;
    private HashScan scan = null;
    private SearchKey key = null;
    private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
//	  throw new UnsupportedOperationException("Not implemented")
    //: Your code here
      this.schema = aSchema;
      this.file = aFile;
      this.heapScan = file.openScan();
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
//	  throw new UnsupportedOperationException("Not implemented");
    //: Your code here
      heapScan.close();
      scan.close();
      heapScan = file.openScan();
      scan = index.openScan(key);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
//	  throw new UnsupportedOperationException("Not implemented");
    //: Your code here
      return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
//	  throw new UnsupportedOperationException("Not implemented");
    //: Your code here
      heapScan.close();
      scan.close();
      isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
//	  throw new UnsupportedOperationException("Not implemented");
    //: Your code here
      return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
//	  throw new UnsupportedOperationException("Not implemented");
    //: Your code here

      byte[] recData = null;
      RID nextRID = scan.getNext();
      RID tempRID = new RID();

      do{
          recData = heapScan.getNext(tempRID);
      } while (!tempRID.equals(nextRID));

      return new Tuple(schema, recData);
  }

} // public class KeyScan extends Iterator
