package simpledb;

import java.io.*;
import java.util.*;

// EXERCISE 5
// Missing: readPage, iterator

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // is this right?
        return file.getAbsoluteFile().hashCode();
   }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;}

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pageSize=BufferPool.getPageSize();
        int offset = pageSize * pid.getPageNumber();
        byte[] myBytes = new byte[pageSize];
        try{
            RandomAccessFile myFile = new RandomAccessFile(file, "r");
            myFile.seek(offset);
            myFile.read(myBytes);
            myFile.close();;
        }
        
        catch(Exception e){
            System.out.println("Error: HeapFile.readPage()");
        }
        try{
            return new HeapPage((HeapPageId) pid, myBytes);
        }
        catch(IOException e){
            System.out.println("Error: HeapFile.readPage() 2");
        }
        catch(NoSuchElementException e){
            System.out.println("Error: HeapFile.readPage() 3");
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try (RandomAccessFile raFile = new RandomAccessFile(file, "rw")) {
            //numPages++;
            long offset = (long) page.getId().getPageNumber() * BufferPool.getPageSize();
            raFile.seek(offset);
            raFile.write(page.getPageData());
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        return (int) (file.length()/pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> writtenPages = new ArrayList<>();

        int numPages = this.numPages();
        for(int i = 0; i < numPages; ++i){
            HeapPageId hid = new HeapPageId(getId(), i);
            // access the page corresponding to the heappage index
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hid, Permissions.READ_WRITE);

            // for each page, if has slots
            // write the page
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                writtenPages.add(page);
                break;
            }
        }
        // if I never exited the loop
        // i.e. no page written
        if (writtenPages.size() == 0){
            HeapPageId hpid = new HeapPageId(getId(), numPages);

            HeapPage blankPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
            writePage(blankPage);

            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            writtenPages.add(newPage);
        }

        return writtenPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	try {
    		return new HeapFileIterator(this, tid);
    	} catch (Exception e) {
    		System.out.println("Error: HeapFile.iterator()");
    	}
    	
        return null;
    }

}




































