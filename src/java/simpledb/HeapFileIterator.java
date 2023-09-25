package simpledb;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	HeapFile myFile;
	
	//BufferPool.getPage(TransactionId tid, PageId pid, Permissions perm)
	TransactionId tid;
	Permissions perm = Permissions.READ_ONLY;
	
	//HeapPageId(tableId, pgNo)
	int tableId;
	
	int currentPage;
	int totalPages;
	
	
	Iterator<Tuple> myTupleIt;
	
	public HeapFileIterator (HeapFile hf, TransactionId tid) throws DbException, TransactionAbortedException {
		this.myFile=hf;
		this.tid=tid;
		this.tableId=myFile.getId();
		this.totalPages=hf.numPages();
		//this.open();
		
	}

	public void changePage() throws TransactionAbortedException, DbException {
		currentPage++;
		HeapPageId hpid = new HeapPageId(tableId, currentPage);
		HeapPage myPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, perm);

		while(myPage==null) {
			currentPage++;
			if(currentPage==totalPages) throw new DbException("Error: HeapFileIterator.changePage()");
			hpid = new HeapPageId(tableId, currentPage);
			myPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid, perm);
		}
		
		this.myTupleIt = myPage.iterator();
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		this.currentPage=-1;
		this.changePage();
		
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(myTupleIt==null) return false;
		if(myTupleIt.hasNext()) return true;
		if(currentPage<totalPages-1) return true;
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if(myTupleIt==null) throw new NoSuchElementException();
		if(myTupleIt.hasNext()) {
			Tuple t = myTupleIt.next();
			return t;
			
		}
		if (!this.hasNext()) throw new NoSuchElementException();
		
		this.changePage();
		return this.next();
		
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		this.close();
		this.open();

	}

	@Override
	public void close() {
		myTupleIt=null;
	}

}
