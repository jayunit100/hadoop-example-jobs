package org.rhbd.others;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.log4j.spi.LoggerFactory;
import org.slf4j.Logger;

public class InputStreamReaderDebuggable extends InputStreamReader{

	public InputStreamReaderDebuggable(InputStream in) {
		super(in);
	}

	public InputStreamReaderDebuggable(InputStream in, Charset cs) {
		super(in, cs);
		// TODO Auto-generated constructor stub
	}

	public InputStreamReaderDebuggable(InputStream in, CharsetDecoder dec) {
		super(in, dec);
		// TODO Auto-generated constructor stub
	}

	public InputStreamReaderDebuggable(InputStream in, String charsetName)
			throws UnsupportedEncodingException {
		super(in, charsetName);
		// TODO Auto-generated constructor stub
	}

	long id = System.currentTimeMillis();
	Logger log = org.slf4j.LoggerFactory.getLogger(InputStreamReaderDebuggable.class);
	@Override
	public void close() throws IOException {
		log.info("InputStreamReader" + id+" is Closing !");
		try{
			super.close();
		}
		catch(Throwable t){
			throw new RuntimeException("[close] Superclass failed "+t.getMessage());
		}
	}

	@Override
	public String getEncoding() {
		try{
			return super.getEncoding();
		}
		catch(Throwable t){
			throw new RuntimeException("[getEnc] Superclass failed "+t.getMessage());
		}
	}

	@Override
	public int read() throws IOException {
		try{
			log.info("InputStreamReader" + id+" [read] ");
		// TODO Auto-generated method stub
		return super.read();
		}
		catch(Throwable t){
			throw new RuntimeException("[read] Superclass failed ! " + t.getMessage());
		}
	}

	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException {
		try{
			log.info("InputStreamReader" + id+" [read] "+offset +" "+length);
			return super.read(cbuf, offset, length);
		}
		catch(Throwable t){
			throw new RuntimeException("[read x y] Superclass failed " + t.getMessage());
		}
	}

	@Override
	public boolean ready() throws IOException {
		try{
			log.info("InputStreamReader" + id+" ready?");
			return super.ready();
		}
		catch(Throwable t){
			throw new RuntimeException("[ready] Superclass failed " + t.getMessage());
		}
	}

}
