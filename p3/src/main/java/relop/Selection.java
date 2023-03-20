package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

    private Iterator iter = null;
    private Predicate[] preds = null;
    //boolean hasNext;
    Tuple next = null;
    private boolean consumed;
    private boolean isOpen;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator aIter, Predicate... aPreds) {
    this.schema = aIter.getSchema();
    this.iter = aIter;
    this.preds = aPreds; 
    this.consumed = false;
    this.isOpen = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
    //Your code here
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    // Your code here
    iter.restart();
    this.consumed = false;
    this.isOpen = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    // Your code here
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    // Your code here
    iter.close();
    isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    // Your code here
    if (consumed)
      return true;

    if (!iter.hasNext())
      return false;

    while (true) {
      next = iter.getNext();

      // check for all predicates
      for (int i = 0; i < preds.length; i++) {
        if (preds[i].evaluate(next)) {
          consumed = true;
          return true;
        }
      }

      if (!iter.hasNext()){
        return false;
      }
    } // end while
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    // Your code here
    if (consumed) {
      consumed = false;
      return next;
    } else {  // not consumed/processed
      if (hasNext()) {  // process now
        consumed = false;
        return next;
      } else {
        throw new IllegalStateException("ERROR: Selection no more tuples!");
      }
    }
  }

} // public class Selection extends Iterator
