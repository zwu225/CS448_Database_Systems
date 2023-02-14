package bufmgr;

import java.util.*;

import diskmgr.DiskMgrException;
import global.Minibase;
import global.Page;
import global.PageId;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a main memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 */
public class BufMgr {

    // INSTANCE VARIABLES
    // Some of these are accessed during the test cases
	// DO NOT CHANGE THE BELOW INSTANCE VARIABLE NAMES - you should use them as appropriate in your code
	// You may add additional ones as you need to

    /** bufPool: the buffer pool. An array of Page objects */
	private Page[] bufPool = null;

    /** frmDescr: An arracy of FrameDescriptor objects, holding information about the contents of each frame. */
    private FrameDescriptor[] frmDescr = null;

    /** numOfFrames: the number of frames used for the maximum capacity of the buffer pool */
    private int numOfFrames = -1;

	/** replacementPolicy: will be set to FIFO when the constructor is called. */
    private static String replacementPolicy = "FIFO";

    /** map: Hash table to track which frame in the buffer a page is in <Key: PageID, Value: Frame> */
    private Map<Integer, Integer> pageMap = new HashMap<>();
	//

    /** fifo: queue for FIFO page replacement. All unpinned pages will be stored here, with the first element being
     * the pageID that was unpinned the longest time ago */
	private Queue<Integer> fifo = new LinkedList<>();	//fifo store frame index

	// END OF REQUIRED INSTANCE VARIABLES

	/**
	 * Resets a FrameDescriptor to the default values with no pageID
	 */
	protected void resetFrameDescriptor(int frameId) {
		resetFrameDescriptor(frameId, -1);
	}

	/**
	 * Resets a FrameDescriptor to the default values with the given pageID
	 */
	protected void resetFrameDescriptor(int frameId, int pageno) {
		frmDescr[frameId].pageno = pageno;
		frmDescr[frameId].pinCount = 0;
		frmDescr[frameId].dirtyBit = false;
	}

	/**
	 * Create the BufMgr object. Allocate pages (frames) for the buffer pool in main
	 * memory and make the buffer manage aware that the replacement policy is
	 * specified by replacerArg (e.g., LH, Clock, LRU, MRU, LIRS, etc.).
	 *
	 * @param numbufs
	 *            number of buffers in the buffer pool
	 * @param lookAheadSize
	 *            number of pages to be looked ahead - can be ignored for this assignment
	 * @param replacementPolicy
	 *            Name of the replacement policy
	 */
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
		// we ignore replacementPolicy as there is only one policy implemented in the
		// system
		numOfFrames = numbufs;
		bufPool = new Page[numOfFrames];
		frmDescr = new FrameDescriptor[numOfFrames];
		this.replacementPolicy = replacementPolicy;
		for (int i = 0; i < numOfFrames; i++) {
			bufPool[i] = new Page();
			frmDescr[i] = new FrameDescriptor();
			resetFrameDescriptor(i, -1);
			fifo.add(i);
		}
	}

	/**
	 * Pin a page. First check if this page is already in the buffer pool. If it is,
	 * increment the pin_count and return a pointer to this page. If the pin_count
	 * was 0 before the call, the page was a replacement candidate, but is no longer
	 * a candidate. If the page is not in the pool, choose a frame (from the set of
	 * replacement candidates) to hold this page, read the page (using the
	 * appropriate method from diskmgr package) and pin it. Also, must write out the
	 * old page in chosen frame if it is dirty before reading new page.(You can
	 * assume that emptyPage==false for this assignment.)
	 *
	 * @param pageno
	 *            page number in the Minibase.
	 * @param page
	 *            the pointer point to the page.
	 * @throws BufferPoolExceededException if there are no valid replacement candidates when attempting to pin a page not already in memory
	 * @throws DiskMgrException if there is an error from the DiskMgr layer. This is likely caused by incorrect implementations of other methods in the BufferManager
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws BufferPoolExceededException, DiskMgrException {
		// TODO YOUR CODE HERE
		if (pageMap.containsKey(pageno.pid)) {			// page is in the frame
			int frameNum = pageMap.get(pageno.pid);			// get the frame number
			page.setPage(bufPool[frameNum]);				// return the page pointer
			if (frmDescr[frameNum].pinCount == 0) {			// the current pinCount is 0
				fifo.remove(frameNum);						// remote the unpinned page from fifo
			}
			frmDescr[frameNum].pinCount += 1;				// increment pinCount
		} else {                                        // page is not in buffer pool
			int newFrameNum;
			try {
				newFrameNum = fifo.remove();
			} catch (NoSuchElementException e) {
				throw new BufferPoolExceededException("ERROR: NO VALID REPLACEMENT CANDIDATES!");
			}

			Minibase.DiskManager.read_page(pageno, page);    // read the page using pageno from Minibase

			if (frmDescr[newFrameNum].pageno == -1) {		// empty frame
				resetFrameDescriptor(newFrameNum, pageno.pid);
				frmDescr[newFrameNum].pinCount += 1;
				bufPool[newFrameNum] = page;
				page.setPage(bufPool[newFrameNum]);
				pageMap.put(pageno.pid, newFrameNum);
			} else {										// not empty frame
				if (frmDescr[newFrameNum].dirtyBit) {			// dirty bit is true
					PageId oldPageId = new PageId(frmDescr[newFrameNum].pageno);
					Minibase.DiskManager.write_page(oldPageId,bufPool[newFrameNum]);	// write back the dirty old page
				}
				pageMap.remove(frmDescr[newFrameNum].pageno);	// remove old <key, value> pair
				resetFrameDescriptor(newFrameNum, pageno.pid);
				frmDescr[newFrameNum].pinCount += 1;
				bufPool[newFrameNum] = page;
				page.setPage(bufPool[newFrameNum]);
				pageMap.put(pageno.pid, newFrameNum);
			}
			page.setPage(bufPool[newFrameNum]);				// set page pointer to the frame
		}
    }

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty==true if the client has modified the page. If so, this call should set
	 * the dirty bit for this frame. Further, if pin_count>0, this method should
	 * decrement it. If pin_count=0 before this call, throw an exception to report
	 * error. (For testing purposes, we ask you to throw an exception named
	 * PageUnpinnedException in case of error.)
	 *
	 * @param pageno
	 *            the PageID of the page
	 * @param dirty
	 *            whether or not the page is dirty
	 * @throws PageNotFoundException the page is not in memory
	 * @throws PageUnpinnedException the page is already unpinned
	 */
	public void unpinPage(PageId pageno, boolean dirty)
			throws PageNotFoundException, PageUnpinnedException {
        // TODO YOUR CODE HERE
	}

	/**
	 * Allocate new pages. Call DB object to allocate a run of new pages and find a
	 * frame in the buffer pool for the first page and pin it. (This call allows a
	 * client of the Buffer Manager to allocate pages on disk.) If buffer is full,
	 * i.e., you can't find a frame for the first page, ask DB to deallocate all
	 * these pages, and return null.
	 *
	 * @param firstpage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 *
	 * @return the first page id of the new pages.__ null, if error.
	 * @throws DiskMgrException if there is an error from the DiskMgr layer. This is likely caused by incorrect implementations of other methods of the Buffer Manager
	 * @throws BufferPoolExceededException if the new page cannot be pinned after the run is allocated due to the buffer being full. If this exception is thrown, the newly allocated pages should be deallocated
	 */
	public PageId newPage(Page firstpage, int howmany) throws DiskMgrException, BufferPoolExceededException {
        // TODO YOUR CODE HERE
		return null;
	}
	
	/**
	 * This method should be called to delete a page that is on disk. This routine
	 * must call the method in diskmgr package to deallocate the page.
	 *
	 * @param pageno
	 *            the page number in the data base.
	 * @throws PagePinnedException if the page is still pinned
	 * @throws DiskMgrException if there is an error in the DiskMgr layer. This is likely caused by incorrect implementations in other methods of the Buffer Manager
	 */
	public void freePage(PageId pageno) throws PagePinnedException, DiskMgrException {
        // TODO YOUR CODE HERE
	}

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method calls
	 * the write_page method of the diskmgr package.
	 *
	 * @param pageid
	 *            the page number in the database.
	 * @throws PageNotFoundException if the page is not in memory
	 * @throws DiskMgrException if there is an error in the DiskMgr layer. This is likely caused by incorrect implementations in other methods of the Buffer Manager
	 */
	public void flushPage(PageId pageid) throws PageNotFoundException, DiskMgrException {
		// find the frame holding that page
		Integer frameId = pageMap.get(pageid.pid);
		if (frameId == null) {
			throw new PageNotFoundException(
					"BufMgr.flushPage: Page with id " + pageid.pid + " does not exist in the buffer bool.");
		} else {
			Minibase.DiskManager.write_page(pageid, bufPool[frameId]);
			frmDescr[frameId].dirtyBit = false;
		}
	}

	/**
	 * Used to flush all dirty pages in the buffer pool to disk
	 * @throws DiskMgrException if there is an error in the DiskMgr layer. This is likely caused by incorrect implementations in other methods of the Buffer Manager
	 */
	public void flushAllPages() throws DiskMgrException {
		for (int i = 0; i < numOfFrames; i++) {
			if (frmDescr[i].dirtyBit == true) {
				Minibase.DiskManager.write_page(new PageId(frmDescr[i].pageno), bufPool[i]);
				frmDescr[i].dirtyBit = false;
			}
		}
	}

	/**
	 * Returns the total number of buffer frames.
	 */
	public int getNumBuffers() {
		return numOfFrames;
	}

	/**
	 * Returns the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
		int numUnpinned = 0;
		for (int i = 0; i < numOfFrames; i++) {
			if (frmDescr[i].pinCount <= 0) {
				numUnpinned++;
			}
		}
		return numUnpinned;
	}

	//*** DO NOT CHANGE ANY EXISTING METHODS BELOW THIS LINE ***
	// Accessor methods for use in test cases
	public FrameDescriptor getFrameDesc(int frameNum) {
		return frmDescr[frameNum];
	}

	public Page getPageFromFrame(int frameNum) {
		return bufPool[frameNum];
	}

	public Integer getFrameFromPage(PageId pid) {
		return pageMap.get(new Integer(pid.pid));
	}
}
