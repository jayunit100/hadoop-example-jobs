package org.rhbd.others;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedReaderDebuggable extends BufferedReader {

	long id = System.currentTimeMillis();
	Logger log = LoggerFactory.getLogger(BufferedReaderDebuggable.class);

	public BufferedReaderDebuggable(Reader in, int sz) {
		super(in, sz);
		// TODO Auto-generated constructor stub
	}

	public BufferedReaderDebuggable(Reader in) {
		super(in);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() throws IOException {
		log.info(id+" Closing " );		
		super.close();
	}

	@Override
	public void mark(int readAheadLimit) throws IOException {
		log.info(id+"marking " + readAheadLimit);		
		super.mark(readAheadLimit);
	}

	@Override
	public boolean markSupported() {
		// TODO Auto-generated method stub
		return super.markSupported();
	}

	@Override
	public int read() throws IOException {
		log.info(id+"read()");		

		// TODO Auto-generated method stub
		return super.read();
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		log.info(id+ "read() " + off +  " " + len);		

		// TODO Auto-generated method stub
		return super.read(cbuf, off, len);
	}

	@Override
	public String readLine() throws IOException {
		log.info(id+" readLine() "  );		
		// TODO Auto-generated method stub
		return super.readLine();
	}

	@Override
	public boolean ready() throws IOException {
		log.info(id+ " ready() "  );		
		// TODO Auto-generated method stub
		return super.ready();
	}

	@Override
	public void reset() throws IOException {
		log.info(id+" reset()");		
		// TODO Auto-generated method stub
		super.reset();
	}

	@Override
	public long skip(long n) throws IOException {
		log.info(id+" skip() " +n );		

		// TODO Auto-generated method stub
		return super.skip(n);
	}

	
}
