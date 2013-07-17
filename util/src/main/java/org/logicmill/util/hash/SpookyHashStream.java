package org.logicmill.util.hash;

import java.nio.charset.Charset;

/**
 * A non-cryptographic, 128-bit hash function with a stream interface.<p>
 * SpookyHashStream is a Java implementation of Bob Jenkins' <i>SpookyHash V2</i> 
 * algorithm (see <a href="http://burtleburtle.net/bob/hash/spooky.html">http://burtleburtle.net/bob/hash/spooky.html</a>).
 * SpookyHashStream accepts input data in a series of put operations. allowing data to be digested in heterogeneous fragments. 
 * The computation occurs as data is digested, so hash codes may be computed for streams of arbitrarily length.
 * 
 * Simple usage includes three steps: 
 * <ol>
 * <li>{@code initialize} resets the internal state of the engine</li>
 * <li>one or more {@code put} operations incorporate data into state of the engine</li>
 * <li>{@code hash} computes a hash code for all of the data incorporated since the most recent {@code initialize}</li>
 * </ol>
 * For example:
 * <code><pre>
 * SpookyHashStream spooky = new SpookyHashStream();
 * spooky.put(new short[] { 1, 2, 3, 5, 8 });
 * spooky.put("hello").putFloat(3.14f).putBoolean(true);
 * long[] result = spooky.hash();
 * // hash code in result[0] and result[1]</pre></code>
 * {@code put} methods return {@code this} (the hash engine}, allowing put operations to be
 * chained:
 * <code><pre>
 * spooky.put("hello").put(3.14f).put(true); </pre></code>

 * Note that {@code hash} does not modify the state of the hash 
 * engine; it computes the hash code for all of the 
 * data added with {@code put} operations since the most recent {@code initialize},
 * regardless of whether {@code hash} has been invoked in the
 * interim.<p>
 * 
 * The following code fragment illustrates the use of this class to compute
 * a hash value for the contents of a file.
 * <code><pre>
 * SpookyHashStream spooky = new SpookyHashStream();
 * java.io.FileInputStream in = new java.io.FileInputStream(...);
 * byte[] buffer = new byte[BUFSIZE];
 * spooky.initialize();
 * int numBytes = in.read(buffer);
 * while (numBytes != -1) {
 * 	spooky.put(buffer, 0, numBytes);
 * 	numBytes = in.read(buffer);
 * }
 * long[] result = spooky.hash();</pre></code>
 * This class is not thread-safe; an instance should not be used concurrently by multiple threaads.
 * Like {@link SpookyHash}, the internal state of SpookyHashStream is initialized with a 128-bit seed value,
 * in the form of two long values. A seed value is associated with the engine at construction time. This seed 
 * is used by default when {@link #initialize()} is called. Alternatively, {@link #initialize(long, long)} uses
 * its parameters as a seed.
 * <p>All credit for design and hashing acumen goes to Bob Jenkins.
 * Errors and poor implementation judgment are the responsibility of 
 * the author.<p>
 *
 * @author David Curtis
 * @see <a href="http://burtleburtle.net/bob/hash/spooky.html">http://burtleburtle.net/bob/hash/spooky.html</a>
 * @see SpookyHash
 */
public class SpookyHashStream {
	/*
	 * The internal algorithm (embodied primarily in mixBlock, end, and endPartial methods) operates on
	 * blocks of 12 long values at a time. This implementation accumulates data in a long[] buffer that holds
	 * two such blocks (192 bytes). When the buffer is full, its contents are mixed into the engine state, which is held
	 * in the array hashState. put operations pack their data parameters into the buffer, aligned on boundaries
	 * equivalent to the size of the primitive data type being added. For example, short and short[] data are aligned
	 * on two-byte boundaries; int and int[] on four-byte boundaries, etc. Padding required for alignment is zero-filled.
	 * If the total data incorporated since the last initialize is less than 192 bytes (one complete buffer) when hash is called, 
	 * a more compact algorithm (in smallHash) is used.
	 */	
	private static final int BLOCK_SIZE_LONGS = 12;
	private static final int BLOCK_SIZE_BYTES = BLOCK_SIZE_LONGS * 8;
	/*
	 * From the original SpookyHash C++ implementation by Bob Jenkins
	 */
	private static final long ARBITRARY_BITS = 0xDEADBEEFDEADBEEFL;
	
	/*
	 * The size of the buffer used to accumulate data; holds two blocks
	 */		
	private static final int BUFFER_SIZE_BYTES = BLOCK_SIZE_BYTES * 2;
	private static final int BUFFER_SIZE_LONGS = BLOCK_SIZE_LONGS * 2;
	
	/*
	 * Masks used for packing primitive types into longs
	 */
	static private final long BYTE_MASK = 0xFFL;
	static private final long SHORT_MASK = 0xFFFFL;
	static private final long INT_MASK = 0xFFFFFFFFL;
	
	static private final Charset UTF8 = Charset.forName("UTF-8");

	/* ---------------- Instance Variables -------------- */

	/*
	 * Default seed value used by initialize()
	 */
	private final long seedValue0;
	private final long seedValue1;
	
	/*
	 * The internal state of the engine.
	 */
	private final long[] hashState;
	
	/*
	 * Count of bytes processed by mixBlock since the most recent initialize
	 */
	private int bytesDigested;	

	/*
	 * Holds data from put operations pending the next mixBlock or hash
	 */
	private final long[] buffer;
	
	/*
	 * package-protected accessor for test
	 */
	long[] getBuffer() {
		return buffer;
	}
	
	/*
	 * The number of bytes current held in buffer
	 */
	private int byteCount;
	
	/*
	 * package-protected accessor for test
	 */
	int getByteCount() {
		return byteCount;
	}
	
	/*
	 * Updates byteCount and flushes when the buffer is full. The calling
	 * context (put and align operations) ensure that byteCount + n does
	 * not exceed the size of the buffer (that is, a single updateByteCount
	 * won't cause byteCount to wrap around a long boundary).
	 */
	private void updateByteCount(int n) {
		if (byteCount + n > (buffer.length << 3)) {
			throw new IllegalArgumentException();
		}
		byteCount += n;
		if (byteCount == (buffer.length << 3)) {
			flush();
		}
	}
	
	/*
	 * Incorporates the contents of buffer into the hash engine state.
	 */
	private void flush() {
		if (bytesDigested == 0) {
			/*
			 * This is the first time flush has been called since initialize.
			 * Initialization of the internal state has been deferred until now,
			 * to avoid the cost of full initialization when smallHash is used.
			 */
			// hashState[0] contains seedValue0
			hashState[3] = hashState[6] = hashState[9] = hashState[0];
			// hashState[1] contains seedValue1
			hashState[4] = hashState[7] = hashState[10] = hashState[1];
			hashState[2] = hashState[5] = hashState[8] = hashState[11] = ARBITRARY_BITS;			
		}
		mixBlock(buffer, 0, hashState);
		mixBlock(buffer, BLOCK_SIZE_LONGS, hashState);
		bytesDigested += BUFFER_SIZE_BYTES;
		byteCount = 0;		
	}

	private void alignShort() {
		if ((byteCount & 1) != 0) {
			updateByteCount(1);
		}
	}
	
	private void alignInt() {
		int residue = byteCount & 3;
		if ((residue) != 0) {
			updateByteCount(4 - residue);
		}
	}
	
	private void alignLong() {
		int residue = (byteCount & 7);
		if (residue != 0) {
			updateByteCount(8 - residue);
		}
	}
	
	/*
	 * All of the fill operations below pack array slices of their respective 
	 * primitive types into a single long word. In general, the 
	 * results produced by fill methods will be assigned to the target word in the buffer
	 * or combined the target word by a bitwise OR operation, in cases where the target
	 * word is already partially filled. 
	 * In every fill method, the occupied parameter contains the number of bytes already
	 * filled in the target word. On entry, occupied must be a multiple of the 
	 * size of the primitive type being pack (in bytes), that is, the calling context
	 * must call the appropriate align operation before calling fill
	 */
	
	/*
	 * boolean values are packed one per byte; true = 1, false = 0
	 */
	private static long fill(boolean[] src, int start, int length, int occupied) {
		long word = 0L;
		int displacement = occupied << 3;
		switch (length) {
		case 8:
			word |= (long)(src[start+7] ? 1 : 0) << (56 + displacement);
		case 7:
			word |= (long)(src[start+6] ? 1 : 0) << (48 + displacement);
		case 6:
			word |= (long)(src[start+5] ? 1 : 0) << (40 + displacement);
		case 5:
			word |= (long)(src[start+4] ? 1 : 0) << (32 + displacement);
		case 4:
			word |= (long)(src[start+3] ? 1 : 0) << (24 + displacement);
		case 3:
			word |= (long)(src[start+2] ? 1 : 0) << (16 + displacement);
		case 2:
			word |= (long)(src[start+1] ? 1 : 0) << (8 + displacement);
		case 1:
			word |= (long)(src[start] ? 1 : 0) << displacement;
		}	
		return word;
	}
	
	private static long fill(byte[] src, int start, int length, int occupied) {
		long word = 0L;
		int displacement = occupied << 3;
		switch (length) {
		case 8:
			word |= (((long)src[start+7]) & BYTE_MASK) << (56 + displacement);
		case 7:
			word |= (((long)src[start+6]) & BYTE_MASK) << (48 + displacement);
		case 6:
			word |= (((long)src[start+5]) & BYTE_MASK) << (40 + displacement);
		case 5:
			word |= (((long)src[start+4]) & BYTE_MASK) << (32 + displacement);
		case 4:
			word |= (((long)src[start+3]) & BYTE_MASK) << (24 + displacement);
		case 3:
			word |= (((long)src[start+2]) & BYTE_MASK) << (16 + displacement);
		case 2:
			word |= (((long)src[start+1]) & BYTE_MASK) << (8 + displacement);
		case 1:
			word |= (((long)src[start]) & BYTE_MASK) << displacement;
		}	
		return word;
	}

	private long fill(short[] src, int start, int length, int occupied) {
		long word = 0L;
		int displacement = occupied << 4;
		switch (length) {
		case 4:
			word |= (((long)src[start+3]) & SHORT_MASK) << (48 + displacement);
		case 3:
			word |= (((long)src[start+2]) & SHORT_MASK) << (32 + displacement);
		case 2:
			word |= (((long)src[start+1]) & SHORT_MASK) << (16 + displacement);
		case 1:
			word |= (((long)src[start]) & SHORT_MASK) << displacement;
		}	
		return word;
	}
	
	private long fill(CharSequence src, int start, int length, int occupied) {
		long word = 0L;
		int displacement = occupied << 4;
		switch (length) {
		case 4:
			word |= ((long)src.charAt(start+3)) << (48 + displacement);
		case 3:
			word |= ((long)src.charAt(start+2)) << (32 + displacement);
		case 2:
			word |= ((long)src.charAt(start+1)) << (16 + displacement);
		case 1:
			word |= ((long)src.charAt(start)) << displacement;
		}	
		return word;		
	}

	private long fill(char[] src, int start, int length, int occupied) {
		long word = 0L;
		int displacement = occupied << 4;
		switch (length) {
		case 4:
			word |= ((long)src[start+3]) << (48 + displacement);
		case 3:
			word |= ((long)src[start+2]) << (32 + displacement);
		case 2:
			word |= ((long)src[start+1]) << (16 + displacement);
		case 1:
			word |= ((long)src[start]) << displacement;
		}	
		return word;		
	}

	/* ---------------- Implementation Internals -------------- */	
	/**
	 * Calculates hash value for payloads smaller than 192 bytes, 
	 * avoiding the startup cost of the full SpookyHash algorithm 
	 * for small payloads. This method is called automatically 
	 * when appropriate. This method treats the payload as a byte stream, packed into the <code>src</code> array
	 * in little-endian byte order. If <code>size</code> is not
	 * an even multiple of 8, then the least significant <code>(size % 8)</code> bytes in the last long word of 
	 * the payload contain relevant data that will affect the result; the remaining bytes are irrelevant, and
	 * will not affect the result. The beginning of the payload must be aligned on a long word boundary (that is,
	 * the first byte of the payload must be the least significant byte <code>src[offset]</code>).
	 * 
	 * @param src payload for which the hash value is computed
	 * @param offset index of the first array element in <code>src</code> to digest
	 * @param lengthBytes the number of bytes to digest
	 * @param seedResult on entry, the first two elements contain the seed 
	 * value; on exit, the first two elements contain the computed result 
	 */
	private static void smallHash(long[] src, int offset, int lengthBytes, long[] seedResult) {
		
		/*
		 * Most of the awkwardness in this code, such as the use of
		 * individual local variables for the internal state vector
		 * (h0, h1, h2, h3) rather than an array, is the result of performance tuning.
		 * 
		 * See Bob Jenkins' description for discussion of the computation itself.
		 */
		long h0, h1, h2, h3;
		h0 = seedResult[0];
		h1 = seedResult[1];
		h2 = ARBITRARY_BITS;
		h3 = ARBITRARY_BITS;
		
		int remainingBytes = lengthBytes;
		int pos = offset;

		/*
		 * Consume any complete 32-byte blocks
		 */
		while (remainingBytes >= 32) {
			h2 += src[pos++];
			h3 += src[pos++];
			
	        h2 = (h2 << 50) | (h2 >>> 14);  h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 52) | (h3 >>> 12);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 30) | (h0 >>> 34);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 41) | (h1 >>> 23);  h1 += h2;  h3 ^= h1;
	        h2 = (h2 << 54) | (h2 >>> 10);  h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 48) | (h3 >>> 16);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 38) | (h0 >>> 26);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 37) | (h1 >>> 27);  h1 += h2;  h3 ^= h1;
	        h2 = (h2 << 62) | (h2 >>> 2);   h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 34) | (h3 >>> 30);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 5)  | (h0 >>> 59);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 36) | (h1 >>> 28);  h1 += h2;  h3 ^= h1;	
			
			h0 += src[pos++];
			h1 += src[pos++];
			remainingBytes -= 32;
		}
		
		if (remainingBytes >= 16) {
			h2 += src[pos++];
			h3 += src[pos++];
			remainingBytes -= 16;

	        h2 = (h2 << 50) | (h2 >>> 14);  h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 52) | (h3 >>> 12);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 30) | (h0 >>> 34);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 41) | (h1 >>> 23);  h1 += h2;  h3 ^= h1;
	        h2 = (h2 << 54) | (h2 >>> 10);  h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 48) | (h3 >>> 16);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 38) | (h0 >>> 26);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 37) | (h1 >>> 27);  h1 += h2;  h3 ^= h1;
	        h2 = (h2 << 62) | (h2 >>> 2);   h2 += h3;  h0 ^= h2;
	        h3 = (h3 << 34) | (h3 >>> 30);  h3 += h0;  h1 ^= h3;
	        h0 = (h0 << 5)  | (h0 >>> 59);  h0 += h1;  h2 ^= h0;
	        h1 = (h1 << 36) | (h1 >>> 28);  h1 += h2;  h3 ^= h1;	

		}
		
		assert remainingBytes < 16;
		/*
		 * Incorporate the buffer length into the hash result.
		 */
		h3 += ((long)(lengthBytes)) << 56;

		if (remainingBytes >= 8) {
			h2 += src[pos++];
			remainingBytes -= 8;
			if (remainingBytes > 0) {
				/*
				 * Mask off (zero-fill) the bytes that aren't payload.
				 */
				long mask = (1L << (remainingBytes << 3)) - 1;
				h3 += src[pos] & mask;
			}
		} else if (remainingBytes > 0) {
			/*
			 * Mask off (zero-fill) the bytes that aren't payload.
			 */
			long mask = (1L << (remainingBytes << 3)) - 1;
			h2 += src[pos] & mask;
		} else {
			h2 += ARBITRARY_BITS;
			h3 += ARBITRARY_BITS;			
		}

        h3 ^= h2;  h2 = (h2 << 15) | (h2 >>> 49);  h3 += h2;
        h0 ^= h3;  h3 = (h3 << 52) | (h3 >>> 12);  h0 += h3;
        h1 ^= h0;  h0 = (h0 << 26) | (h0 >>> 38);  h1 += h0;
        h2 ^= h1;  h1 = (h1 << 51) | (h1 >>> 13);  h2 += h1;
        h3 ^= h2;  h2 = (h2 << 28) | (h2 >>> 36);  h3 += h2;
        h0 ^= h3;  h3 = (h3 << 9)  | (h3 >>> 55);  h0 += h3;
        h1 ^= h0;  h0 = (h0 << 47) | (h0 >>> 17);  h1 += h0;
        h2 ^= h1;  h1 = (h1 << 54) | (h1 >>> 10);  h2 += h1;
        h3 ^= h2;  h2 = (h2 << 32) | (h2 >>> 32);  h3 += h2;
        h0 ^= h3;  h3 = (h3 << 25) | (h3 >>> 39);  h0 += h3;
        h1 ^= h0;  h0 = (h0 << 63) | (h0 >>> 1);   h1 += h0;
		
        seedResult[0] = h0;
        seedResult[1] = h1;
	}
	

	/**
	 * Digests 96 bytes (12 longs) of input, mixing it into the internal 
	 * state of the engine.
	 * 
	 * @param src contains data to digest
	 * @param start index of the first element in src to digest
	 * @param h internal state of the hash engine
	 */
	private static void mixBlock(long[] src, int start, long[] h) {
		h[0] += src[start]; 
		h[2] ^= h[10];     
		h[11] ^= h[0];   
		h[0] = (h[0] << 11) | (h[0] >>> 53);     
		h[11] += h[1];
		
		h[1] += src[start+1]; 
		h[3] ^= h[11];     
		h[0] ^= h[1];     
		h[1] = (h[1] << 32) | (h[1] >>> 32);     
		h[0] += h[2];

		h[2] += src[start+2]; 
		h[4] ^= h[0];      
		h[1] ^= h[2];     
		h[2] = (h[2] << 43) | (h[2] >>> 21);     
		h[1] += h[3];
	
		h[3] += src[start+3]; 
		h[5] ^= h[1];      
		h[2] ^= h[3];     
		h[3] = (h[3] << 31) | (h[3] >>> 33);     
		h[2] += h[4];

		h[4] += src[start+4]; 
		h[6] ^= h[2];      
		h[3] ^= h[4];     
		h[4] = (h[4] << 17) | (h[4] >>> 47);     
		h[3] += h[5];

		h[5] += src[start+5]; 
		h[7] ^= h[3];      
		h[4] ^= h[5];     
		h[5] = (h[5] << 28) | (h[5] >>> 36);     
		h[4] += h[6];

		h[6] += src[start+6]; 
		h[8] ^= h[4];      
		h[5] ^= h[6];     
		h[6] = (h[6] << 39) | (h[6] >>> 25);     
		h[5] += h[7];

		h[7] += src[start+7]; 
		h[9] ^= h[5];      
		h[6] ^= h[7];     
		h[7] = (h[7] << 57) | (h[7] >>> 7);     
		h[6] += h[8];

		h[8] += src[start+8]; 
		h[10] ^= h[6];     
		h[7] ^= h[8];     
		h[8] = (h[8] << 55) | (h[8] >>> 9);     
		h[7] += h[9];

		h[9] += src[start+9]; 
		h[11] ^= h[7];     
		h[8] ^= h[9];     
		h[9] = (h[9] << 54) | (h[9] >>> 10);     
		h[8] += h[10];

		h[10] += src[start+10]; 
		h[0] ^= h[8];      
		h[9] ^= h[10];    
		h[10] = (h[10] << 22) | (h[10] >>> 42);  
		h[9] += h[11];

		h[11] += src[start+11]; 
		h[1] ^= h[9];      
		h[10] ^= h[11];   
		h[11] = (h[11] << 46) | (h[11] >>> 18);  
		h[10] += h[0];		
	}
	
	/**
	 * Digests a partial block (less than 96 bytes) of input and performs a final mixing
	 * operation.
	 * 
	 * @param src contains bytes to be digested
	 * @param start index of the first byte to be digested
	 * @param remainingBytes number of bytes to be digested
	 * @param h internal state of the hash engine
	 * greater
	 */
	protected static void end(long[] src, int start, int remainingBytes, long[] h) {
	
		int remainingLongs = remainingBytes >>> 3;
		int partialBytes = remainingBytes & 7;
		
		switch (remainingLongs) { // fall-through is intentional
		case 11: h[10] += src[start+10];
		case 10: h[9] += src[start+9];
		case 9: h[8] += src[start+8]; 
		case 8: h[7] += src[start+7];
		case 7: h[6] += src[start+6];
		case 6: h[5] += src[start+5];
		case 5: h[4] += src[start+4];
		case 4: h[3] += src[start+3];
		case 3: h[2] += src[start+2];
		case 2: h[1] += src[start+1];
		case 1: h[0] += src[start];
		default:
		}
		
		if (partialBytes > 0) {
			/*
			 * The last word is partial, mask off irrelevant bytes.
			 */
			long mask = (1L << (partialBytes << 3)) - 1;
			long partial = src[start+remainingLongs] & mask;
			switch(remainingLongs) {
			case 11: h[11] += partial; break;
			case 10: h[10] += partial; break;
			case 9: h[9] += partial; break;
			case 8: h[8] += partial; break;
			case 7: h[7] += partial; break;
			case 6: h[6] += partial; break;
			case 5: h[5] += partial; break;
			case 4: h[4] += partial; break;
			case 3: h[3] += partial; break;
			case 2: h[2] += partial; break;
			case 1: h[1] += partial; break;
			case 0: h[0] += partial; break;
			}
		}
		h[11] += ((long)remainingBytes) << 56;
		endPartial(h);
		endPartial(h);
		endPartial(h);
	}
	
	/**
	 * Performs one pass of final mixing on the internal state.
	 * 
	 * @param h internal state of the hash engine
	 */
	protected static void endPartial(long[] h) {
        h[11] += h[1];   
        h[2] ^= h[11];   
        h[1] = (h[1] << 44)  | (h[1] >>> 20);
        h[0] += h[2];    
        h[3] ^= h[0];    
        h[2] = (h[2] << 15)  | (h[2] >>> 49);
        h[1] += h[3];    
        h[4] ^= h[1];    
        h[3] = (h[3] << 34)  | (h[3] >>> 30);
        h[2] += h[4];    
        h[5] ^= h[2];    
        h[4] = (h[4] << 21)  | (h[4] >>> 43);
        h[3] += h[5];    
        h[6] ^= h[3];    
        h[5] = (h[5] << 38)  | (h[5] >>> 26);
        h[4] += h[6];    
        h[7] ^= h[4];    
        h[6] = (h[6] << 33)  | (h[6] >>> 31);
        h[5] += h[7];    
        h[8] ^= h[5];    
        h[7] = (h[7] << 10)  | (h[7] >>> 54);
        h[6] += h[8];    
        h[9] ^= h[6];    
        h[8] = (h[8] << 13)  | (h[8] >>> 51);
        h[7] += h[9];    
        h[10] ^= h[7];    
        h[9] = (h[9] << 38)  | (h[9] >>> 26);
        h[8] += h[10];   
        h[11] ^= h[8];    
        h[10] = (h[10] << 53) | (h[10] >>> 11);
        h[9] += h[11];   
        h[0] ^= h[9];    
        h[11] = (h[11] << 42) | (h[11] >>> 22);
        h[10] += h[0];   
        h[1] ^= h[10];   
        h[0] = (h[0] << 54)  | (h[0] >>> 10);		
	}


	
	/* ---------------- Public Methods -------------- */	
	
	/**
	 * Constructs a new hash engine, with a
	 * default seed value of <code>{0L, 0L}</code>.
	 */
	public SpookyHashStream() {
		this(0L, 0L);
	}
	
	/**
	 * Constructs a new hash engine, with the
	 * specified default seed value.
	 * 
	 * @param seed0 first half of the default seed value
	 * @param seed1 second half of the default seed value
	 */
	public SpookyHashStream(long seed0, long seed1) {
		buffer = new long[BUFFER_SIZE_LONGS];
		seedValue0 = seed0;
		seedValue1 = seed1;
		hashState = new long[BLOCK_SIZE_LONGS];
		bytesDigested = 0;
		byteCount = 0;
	}

	/** 
	 * Initializes the state of the hash engine with the specified seed value. 
	 * Any buffered data retained from previous <code>put</code> 
	 * operations will be discarded.
	 * 
	 * @param seed0 first half of the seed value
	 * @param seed1 second half of the seed value
	 */
	public void initialize(long seed0, long seed1) {
		bytesDigested = 0;
		byteCount = 0;
		hashState[0] = seed0;
		hashState[1] = seed1;
	}
	
	
	/**
	 * Initializes the state of the hash engine with the default seed value
	 * associated with this instance. Any undigested payload retained from 
	 * previous <code>update</code> operations will be discarded.
	 */
	public void initialize() {
		initialize(seedValue0, seedValue1);
	}
	
	/**
	 * Incorporates data from an array of {@code byte} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(byte[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}

		int occupied = (byteCount & 7);
		if (occupied > 0) {
			int unused = 8 - occupied;
			int fillCount = (remaining > unused) ? unused : remaining;
			buffer[(byteCount >>> 3)] |= fill(src, pos, fillCount, occupied);
			updateByteCount(fillCount);
			remaining -= fillCount;
			pos += fillCount;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 8) {
			buffer[(byteCount >>> 3)] = fill(src, pos, 8, 0);
			updateByteCount(8);
			remaining -= 8;
			pos += 8;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = fill(src, pos, remaining, 0);
			updateByteCount(remaining);
		}
		return this;	
	}
	
	/** Incorporates an array of {@code byte} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(byte[] src) {
		return put(src, 0, src.length);
	}
	

	/** Incorporates a single {@code byte} value into the hash engine state.
	 * @param b the {@code byte} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putByte(byte b) {
		int occupied = (byteCount & 7);
		if (occupied == 0) {
			buffer[(byteCount >>> 3)] = ((long)b & BYTE_MASK);					
		} else {
			buffer[(byteCount >>> 3)] |= ((long)b & BYTE_MASK) << (occupied << 3);					
		}
		updateByteCount(1);
		return this;
	}

	/**
	 * Incorporates data from an array of {@code boolean} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(boolean[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		int occupied = (byteCount & 7);
		if (occupied > 0) {
			int unused = 8 - occupied;
			int fillCount = (remaining > unused) ? unused : remaining;
			buffer[(byteCount >>> 3)] |= fill(src, pos, fillCount, occupied);
			updateByteCount(fillCount);
			remaining -= fillCount;
			pos += fillCount;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 8) {
			buffer[(byteCount >>> 3)] = fill(src, pos, 8, 0);
			updateByteCount(8);
			remaining -= 8;
			pos += 8;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = fill(src, pos, remaining, 0);
			updateByteCount(remaining);
		}
		return this;
	}
	
	/** Incorporates an array of {@code boolean} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(boolean[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code boolean} value into the hash engine state.
	 * @param b the {@code boolean} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putBoolean(boolean b) {
		int occupied = (byteCount & 7);
		if (occupied == 0) {
			buffer[(byteCount >>> 3)] = (long)(b ? 1 : 0);					
		} else {
			buffer[(byteCount >>> 3)] |= (long)(b ? 1 : 0) << (occupied << 3);					
		}
		updateByteCount(1);
		return this;
	}
			
	/**
	 * Incorporates data from an array of {@code short} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(short[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignShort();		
		int occupied = (byteCount & 7) >>> 1;
		if (occupied > 0) {
			int unused = 4 - occupied;				
			int fillCount = 0;
			if (remaining > unused) {
				fillCount = unused;
			} else {
				fillCount = remaining;
			}
			buffer[(byteCount >>> 3)] |= fill(src, pos, fillCount, occupied);
			updateByteCount(fillCount<<1);
			remaining -= fillCount;
			pos += fillCount;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 4) {
			buffer[(byteCount >>> 3)] = fill(src, pos, 4, 0);
			updateByteCount(8);
			remaining -= 4;
			pos += 4;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = fill(src, pos, remaining, 0);
			updateByteCount(remaining<<1);
		}
		return this;
	}
	
	/** Incorporates an array of {@code short} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(short[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code short} value into the hash engine state.
	 * @param s the {@code short} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putShort(short s) {
		alignShort();
		int occupied = (byteCount & 7);
		if (occupied == 0) {
			buffer[(byteCount >>> 3)] = ((long)s) & SHORT_MASK;
		} else {
			buffer[(byteCount >>> 3)] |= (((long)s) & SHORT_MASK) << (occupied << 3);		
		}
		updateByteCount(2);
		return this;
	}

	/**
	 * Incorporates data from an array of {@code char} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(char[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignShort();		
		int occupied = (byteCount & 7) >>> 1;
		if (occupied > 0) {
			int unused = 4 - occupied;				
			int fillCount = (remaining > unused) ? unused : remaining;
			buffer[(byteCount >>> 3)] |= fill(src, pos, fillCount, occupied);
			updateByteCount(fillCount<<1);
			remaining -= fillCount;
			pos += fillCount;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 4) {
			buffer[(byteCount >>> 3)] = fill(src, pos, 4, 0);
			updateByteCount(8);
			remaining -= 4;
			pos += 4;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = fill(src, pos, remaining, 0);
			updateByteCount(remaining<<1);
		}
		return this;
	}
	
	/** Incorporates an array of {@code char} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(char[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a CharSequence into the hash engine state. Note that
	 * CharSequence is a superclass of String, so String parameters may be
	 * passed directly to this method.
	 * @param src the CharSequence incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(CharSequence src) {
		int pos = 0;
		int remaining = src.length();
		alignShort();		
		int occupied = (byteCount & 7) >>> 1;
		if (occupied > 0) {
			int unused = 4 - occupied;				
			int fillCount = (remaining > unused) ? unused : remaining;
			buffer[(byteCount >>> 3)] |= fill(src, pos, fillCount, occupied);
			updateByteCount(fillCount<<1);
			remaining -= fillCount;
			pos += fillCount;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 4) {
			buffer[(byteCount >>> 3)] = fill(src, pos, 4, 0);
			updateByteCount(8);
			remaining -= 4;
			pos += 4;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = fill(src, pos, remaining, 0);
			updateByteCount(remaining<<1);
		}
		return this;
	}

	/** 
	 * Encodes a string of characters into an array of byte, using the
	 * UTF-8 charset encoding, and incorporates the resulting array of byte
	 * into the hash engine state.
	 * @param src string encoded and incorporated into the hash engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream putUTF8(String src) {
		return put(src.getBytes(UTF8));
	}
	
	/** Incorporates a single {@code char} value into the hash engine state.
	 * @param c the {@code char} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putChar(char c) {
		alignShort();
		int occupied = (byteCount & 7);
		if (occupied == 0) {
			buffer[(byteCount >>> 3)] = ((long)c);
		} else {
			buffer[(byteCount >>> 3)] |= ((long)c) << (occupied << 3);
		}
		updateByteCount(2);
		return this;
	}

	/**
	 * Incorporates data from an array of {@code int} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(int[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignInt();
		if (((byteCount) & 4) != 0) {
			buffer[(byteCount >>> 3)] |= ((long)src[pos]) << 32;
			updateByteCount(4);
			remaining--;
			pos++;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 2) {
			buffer[(byteCount >>> 3)] = (((long)src[pos+1]) << 32) | (((long)src[pos]) & INT_MASK);
			updateByteCount(8);
			remaining -= 2;
			pos += 2;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = ((long)src[pos]) & INT_MASK;
			updateByteCount(4);
		}
		return this;
	}
	
	/** Incorporates an array of {@code int} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(int[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code int} value into the hash engine state.
	 * @param i the {@code int} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putInt(int i) {
		alignInt();
		int occupied = (byteCount & 7);
		if (occupied == 0) {
			buffer[(byteCount >>> 3)] = (long)i & INT_MASK;
		} else {
			buffer[(byteCount >>> 3)] |= ((long)i & INT_MASK) << (occupied << 3);
		}
		updateByteCount(4);
		return this;
	}
	
	/**
	 * Incorporates data from an array of {@code float} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(float[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignInt();
		if (((byteCount) & 4) != 0) {
			buffer[(byteCount >>> 3)] |= ((long)src[pos]) << 32;
			updateByteCount(4);
			remaining--;
			pos++;
		}
		// assert currentBytes % 8 == 0 || remaining == 0;
		while (remaining >= 2) {
			buffer[(byteCount >>> 3)] = (((long)Float.floatToRawIntBits(src[pos+1])) << 32) 
					| (((long)Float.floatToRawIntBits(src[pos])) & INT_MASK);
			updateByteCount(8);
			remaining -= 2;
			pos += 2;
		}
		if (remaining > 0) {
			buffer[(byteCount >>> 3)] = ((long)Float.floatToRawIntBits(src[pos])) & INT_MASK;
			updateByteCount(4);
		}
		return this;
	}
	
	/** Incorporates an array of {@code float} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(float[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code float} value into the hash engine state.
	 * @param f the {@code float} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putFloat(float f) {
		return putInt(Float.floatToRawIntBits(f));
	}
	
	
	/**
	 * Incorporates data from an array of {@code long} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(long[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignLong();
		while (remaining > 0) {
			buffer[(byteCount >>> 3)] = src[pos++];
			remaining--;
			updateByteCount(8);
		}
		return this;
	}
	
	/** Incorporates an array of {@code long} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(long[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code long} value into the hash engine state.
	 * @param l the {@code long} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putLong(long l) {
		alignLong();
		buffer[(byteCount >>> 3)] = l;
		updateByteCount(8);
		return this;
	}
	
	/**
	 * Incorporates data from an array of {@code double} into the hash engine state.
	 * @param src contains the data to incorporate
	 * @param pos index of the first element of src to incorporate
	 * @param remaining the number of elements in src to incorporate
	 * @return this (the engine)
	 */
	public SpookyHashStream put(double[] src, int pos, int remaining) {
		if (pos < 0 || remaining < 0 || remaining > (src.length - pos)) {
			throw new IndexOutOfBoundsException();
		}
		alignLong();
		while (remaining > 0) {
			buffer[(byteCount >>> 3)] = Double.doubleToRawLongBits(src[pos++]);
			remaining--;
			updateByteCount(8);
		}
		return this;
	}

	/** Incorporates an array of {@code double} into the hash engine state.
	 * @param src the array incorporated into the engine state
	 * @return this (the engine)
	 */
	public SpookyHashStream put(double[] src) {
		return put(src, 0, src.length);
	}
	
	/** Incorporates a single {@code double} value into the hash engine state.
	 * @param d the {@code double} value to incorporate.
	 * @return this (the engine)
	 */
	public SpookyHashStream putDouble(double d) {
		return putLong(Double.doubleToRawLongBits(d));
	}
		
	/**
	 * Computes the hash code for all data incorporated
	 * since the most recent <code>initialize</code> operation. 
	 * {@code hash} does not modify the state of the engine. 
	 * @return contains the computed hash code in the first two elements
	 */
	public long[] hash() {
		if (bytesDigested == 0) {
			/*
			 * buffer contains all data from put operations since the 
			 * most recent initialize (also, byteCount < 192) so use
			 * smallHash. Since mixBlock hasn't been called (since
			 * initialize), hashState[0] and [1] contain the seed.
			 */
			long[] seedResult = { hashState[0], hashState[1] };
			smallHash(buffer, 0, byteCount, seedResult);
			return seedResult;
		} else  {
			/*
			 * Clone the engine state to avoid modifying the "real" internal state.
			 */
			long[] clonedState = (long[])hashState.clone();
			if (byteCount >= BLOCK_SIZE_BYTES) {
				mixBlock(buffer, 0, clonedState);
				end(buffer, BLOCK_SIZE_LONGS, byteCount - BLOCK_SIZE_BYTES, clonedState);
			} else {
				// zero-fill the first block and mix
				end(buffer, 0, byteCount, clonedState);
			}
			long[] result = { clonedState[0], clonedState[1] };
			return result;
		}
	}

}
