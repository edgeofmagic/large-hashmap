package org.logicmill.util.hash;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.logicmill.util.hash.SpookyHashStream;

public class SpookyHashStreamTest {
	
	static final int BUF_SIZE = 512;
	static final long RESULT_MASK = 0x00000000FFFFFFFFL;

	/*
	 * This array contains output generated by the C++ reference implementation of SpookyHash. The unit tests
	 * of hash methods use this array to duplicate the tests performed by the TestResults() function in TestSpookyV2.cpp 
	 * (found at http://burtleburtle.net/bob/c/TestSpookyV2.cpp).
	 */
    static final long[] expected = {
    	0x6bf50919L, 0x70de1d26L, 0xa2b37298L, 0x35bc5fbfL, 0x8223b279L, 0x5bcb315eL, 0x53fe88a1L, 0xf9f1a233L, 
        0xee193982L, 0x54f86f29L, 0xc8772d36L, 0x9ed60886L, 0x5f23d1daL, 0x1ed9f474L, 0xf2ef0c89L, 0x83ec01f9L, 
        0xf274736cL, 0x7e9ac0dfL, 0xc7aed250L, 0xb1015811L, 0xe23470f5L, 0x48ac20c4L, 0xe2ab3cd5L, 0x608f8363L, 
        0xd0639e68L, 0xc4e8e7abL, 0x863c7c5bL, 0x4ea63579L, 0x99ae8622L, 0x170c658bL, 0x149ba493L, 0x027bca7cL, 
        0xe5cfc8b6L, 0xce01d9d7L, 0x11103330L, 0x5d1f5ed4L, 0xca720ecbL, 0xef408aecL, 0x733b90ecL, 0x855737a6L, 
        0x9856c65fL, 0x647411f7L, 0x50777c74L, 0xf0f1a8b7L, 0x9d7e55a5L, 0xc68dd371L, 0xfc1af2ccL, 0x75728d0aL, 
        0x390e5fdcL, 0xf389b84cL, 0xfb0ccf23L, 0xc95bad0eL, 0x5b1cb85aL, 0x6bdae14fL, 0x6deb4626L, 0x93047034L, 
        0x6f3266c6L, 0xf529c3bdL, 0x396322e7L, 0x3777d042L, 0x1cd6a5a2L, 0x197b402eL, 0xc28d0d2bL, 0x09c1afb4L, 
        
        0x069c8bb7L, 0x6f9d4e1eL, 0xd2621b5cL, 0xea68108dL, 0x8660cb8fL, 0xd61e6de6L, 0x7fba15c7L, 0xaacfaa97L, 
        0xdb381902L, 0x4ea22649L, 0x5d414a1eL, 0xc3fc5984L, 0xa0fc9e10L, 0x347dc51cL, 0x37545fb6L, 0x8c84b26bL, 
        0xf57efa5dL, 0x56afaf16L, 0xb6e1eb94L, 0x9218536aL, 0xe3cc4967L, 0xd3275ef4L, 0xea63536eL, 0x6086e499L, 
        0xaccadce7L, 0xb0290d82L, 0x4ebfd0d6L, 0x46ccc185L, 0x2eeb10d3L, 0x474e3c8cL, 0x23c84aeeL, 0x3abae1cbL, 
        0x1499b81aL, 0xa2993951L, 0xeed176adL, 0xdfcfe84cL, 0xde4a961fL, 0x4af13fe6L, 0xe0069c42L, 0xc14de8f5L, 
        0x6e02ce8fL, 0x90d19f7fL, 0xbca4a484L, 0xd4efdd63L, 0x780fd504L, 0xe80310e3L, 0x03abbc12L, 0x90023849L, 
        0xd6f6fb84L, 0xd6b354c5L, 0x5b8575f0L, 0x758f14e4L, 0x450de862L, 0x90704afbL, 0x47209a33L, 0xf226b726L, 
        0xf858dab8L, 0x7c0d6de9L, 0xb05ce777L, 0xee5ff2d4L, 0x7acb6d5cL, 0x2d663f85L, 0x41c72a91L, 0x82356bf2L, 
        
        0x94e948ecL, 0xd358d448L, 0xeca7814dL, 0x78cd7950L, 0xd6097277L, 0x97782a5dL, 0xf43fc6f4L, 0x105f0a38L, 
        0x9e170082L, 0x4bfe566bL, 0x4371d25fL, 0xef25a364L, 0x698eb672L, 0x74f850e4L, 0x4678ff99L, 0x4a290dc6L, 
        0x3918f07cL, 0x32c7d9cdL, 0x9f28e0afL, 0x0d3c5a86L, 0x7bfc8a45L, 0xddf0c7e1L, 0xdeacb86bL, 0x970b3c5cL, 
        0x5e29e199L, 0xea28346dL, 0x6b59e71bL, 0xf8a8a46aL, 0x862f6ce4L, 0x3ccb740bL, 0x08761e9eL, 0xbfa01e5fL, 
        0xf17cfa14L, 0x2dbf99fbL, 0x7a0be420L, 0x06137517L, 0xe020b266L, 0xd25bfc61L, 0xff10ed00L, 0x42e6be8bL, 
        0x029ef587L, 0x683b26e0L, 0xb08afc70L, 0x7c1fd59eL, 0xbaae9a70L, 0x98c8c801L, 0xb6e35a26L, 0x57083971L, 
        0x90a6a680L, 0x1b44169eL, 0x1dce237cL, 0x518e0a59L, 0xccb11358L, 0x7b8175fbL, 0xb8fe701aL, 0x10d259bbL, 
        0xe806ce10L, 0x9212be79L, 0x4604ae7bL, 0x7fa22a84L, 0xe715b13aL, 0x0394c3b2L, 0x11efbbaeL, 0xe13d9e19L, 

        0x77e012bdL, 0x2d05114cL, 0xaecf2dddL, 0xb2a2b4aaL, 0xb9429546L, 0x55dce815L, 0xc89138f8L, 0x46dcae20L, 
        0x1f6f7162L, 0x0c557ebcL, 0x5b996932L, 0xafbbe7e2L, 0xd2bd5f62L, 0xff475b9fL, 0x9cec7108L, 0xeaddcffbL, 
        0x5d751aefL, 0xf68f7bdfL, 0xf3f4e246L, 0x00983fcdL, 0x00bc82bbL, 0xbf5fd3e7L, 0xe80c7e2cL, 0x187d8b1fL, 
        0xefafb9a7L, 0x8f27a148L, 0x5c9606a9L, 0xf2d2be3eL, 0xe992d13aL, 0xe4bcd152L, 0xce40b436L, 0x63d6a1fcL, 
        0xdc1455c4L, 0x64641e39L, 0xd83010c9L, 0x2d535ae0L, 0x5b748f3eL, 0xf9a9146bL, 0x80f10294L, 0x2859acd4L, 
        0x5fc846daL, 0x56d190e9L, 0x82167225L, 0x98e4dabaL, 0xbf7865f3L, 0x00da7ae4L, 0x9b7cd126L, 0x644172f8L, 
        0xde40c78fL, 0xe8803efcL, 0xdd331a2bL, 0x48485c3cL, 0x4ed01ddcL, 0x9c0b2d9eL, 0xb1c6e9d7L, 0xd797d43cL, 
        0x274101ffL, 0x3bf7e127L, 0x91ebbc56L, 0x7ffeb321L, 0x4d42096fL, 0xd6e9456aL, 0x0bade318L, 0x2f40ee0bL, 
        
        0x38cebf03L, 0x0cbc2e72L, 0xbf03e704L, 0x7b3e7a9aL, 0x8e985acdL, 0x90917617L, 0x413895f8L, 0xf11dde04L, 
        0xc66f8244L, 0xe5648174L, 0x6c420271L, 0x2469d463L, 0x2540b033L, 0xdc788e7bL, 0xe4140dedL, 0x0990630aL, 
        0xa54abed4L, 0x6e124829L, 0xd940155aL, 0x1c8836f6L, 0x38fda06cL, 0x5207ab69L, 0xf8be9342L, 0x774882a8L, 
        0x56fc0d7eL, 0x53a99d6eL, 0x8241f634L, 0x9490954dL, 0x447130aaL, 0x8cc4a81fL, 0x0868ec83L, 0xc22c642dL, 
        0x47880140L, 0xfbff3becL, 0x0f531f41L, 0xf845a667L, 0x08c15fb7L, 0x1996cd81L, 0x86579103L, 0xe21dd863L, 
        0x513d7f97L, 0x3984a1f1L, 0xdfcdc5f4L, 0x97766a5eL, 0x37e2b1daL, 0x41441f3fL, 0xabd9ddbaL, 0x23b755a9L, 
        0xda937945L, 0x103e650eL, 0x3eef7c8fL, 0x2760ff8dL, 0x2493a4cdL, 0x1d671225L, 0x3bf4bd4cL, 0xed6e1728L, 
        0xc70e9e30L, 0x4e05e529L, 0x928d5aa6L, 0x164d0220L, 0xb5184306L, 0x4bd7efb3L, 0x63830f11L, 0xf3a1526cL, 
        
        0xf1545450L, 0xd41d5df5L, 0x25a5060dL, 0x77b368daL, 0x4fe33c7eL, 0xeae09021L, 0xfdb053c4L, 0x2930f18dL, 
        0xd37109ffL, 0x8511a781L, 0xc7e7cdd7L, 0x6aeabc45L, 0xebbeaeaaL, 0x9a0c4f11L, 0xda252cbbL, 0x5b248f41L, 
        0x5223b5ebL, 0xe32ab782L, 0x8e6a1c97L, 0x11d3f454L, 0x3e05bd16L, 0x0059001dL, 0xce13ac97L, 0xf83b2b4cL, 
        0x71db5c9aL, 0xdc8655a6L, 0x9e98597bL, 0x3fcae0a2L, 0x75e63ccdL, 0x076c72dfL, 0x4754c6adL, 0x26b5627bL, 
        0xd818c697L, 0x998d5f3dL, 0xe94fc7b2L, 0x1f49ad1aL, 0xca7ff4eaL, 0x9fe72c05L, 0xfbd0cbbfL, 0xb0388cebL, 
        0xb76031e3L, 0xd0f53973L, 0xfb17907cL, 0xa4c4c10fL, 0x9f2d8af9L, 0xca0e56b0L, 0xb0d9b689L, 0xfcbf37a3L, 
        0xfede8f7dL, 0xf836511cL, 0x744003fcL, 0x89eba576L, 0xcfdcf6a6L, 0xc2007f52L, 0xaaaf683fL, 0x62d2f9caL, 
        0xc996f77fL, 0x77a7b5b3L, 0x8ba7d0a4L, 0xef6a0819L, 0xa0d903c0L, 0x01b27431L, 0x58fffd4cL, 0x4827f45cL, 
        
        0x44eb5634L, 0xae70edfcL, 0x591c740bL, 0x478bf338L, 0x2f3b513bL, 0x67bf518eL, 0x6fef4a0cL, 0x1e0b6917L, 
        0x5ac0edc5L, 0x2e328498L, 0x077de7d5L, 0x5726020bL, 0x2aeda888L, 0x45b637caL, 0xcf60858dL, 0x3dc91ae2L, 
        0x3e6d5294L, 0xe6900d39L, 0x0f634c71L, 0x827a5fa4L, 0xc713994bL, 0x1c363494L, 0x3d43b615L, 0xe5fe7d15L, 
        0xf6ada4f2L, 0x472099d5L, 0x04360d39L, 0x7f2a71d0L, 0x88a4f5ffL, 0x2c28fac5L, 0x4cd64801L, 0xfd78dd33L, 
        0xc9bdd233L, 0x21e266ccL, 0x9bbf419dL, 0xcbf7d81dL, 0x80f15f96L, 0x04242657L, 0x53fb0f66L, 0xded11e46L, 
        0xf2fdba97L, 0x8d45c9f1L, 0x4eeae802L, 0x17003659L, 0xb9db81a7L, 0xe734b1b2L, 0x9503c54eL, 0xb7c77c3eL, 
        0x271dd0abL, 0xd8b906b5L, 0x0d540ec6L, 0xf03b86e0L, 0x0fdb7d18L, 0x95e261afL, 0xad9ec04eL, 0x381f4a64L, 
        0xfec798d7L, 0x09ea20beL, 0x0ef4ca57L, 0x1e6195bbL, 0xfd0da78bL, 0xcea1653bL, 0x157d9777L, 0xf04af50fL, 
        
        0xad7baa23L, 0xd181714aL, 0x9bbdab78L, 0x6c7d1577L, 0x645eb1e7L, 0xa0648264L, 0x35839ca6L, 0x2287ef45L, 
        0x32a64ca3L, 0x26111f6fL, 0x64814946L, 0xb0cddaf1L, 0x4351c59eL, 0x1b30471cL, 0xb970788aL, 0x30e9f597L, 
        0xd7e58df1L, 0xc6d2b953L, 0xf5f37cf4L, 0x3d7c419eL, 0xf91ecb2dL, 0x9c87fd5dL, 0xb22384ceL, 0x8c7ac51cL, 
        0x62c96801L, 0x57e54091L, 0x964536feL, 0x13d3b189L, 0x4afd1580L, 0xeba62239L, 0xb82ea667L, 0xae18d43aL, 
        0xbef04402L, 0x1942534fL, 0xc54bf260L, 0x3c8267f5L, 0xa1020dddL, 0x112fcc8aL, 0xde596266L, 0xe91d0856L, 
        0xf300c914L, 0xed84478eL, 0x5b65009eL, 0x4764da16L, 0xaf8e07a2L, 0x4088dc2cL, 0x9a0cad41L, 0x2c3f179bL, 
        0xa67b83f7L, 0xf27eab09L, 0xdbe10e28L, 0xf04c911fL, 0xd1169f87L, 0x8e1e4976L, 0x17f57744L, 0xe4f5a33fL, 
        0x27c2e04bL, 0x0b7523bdL, 0x07305776L, 0xc6be7503L, 0x918fa7c9L, 0xaf2e2cd9L, 0x82046f8eL, 0xcc1c8250L
    };

    /*
     * Payload buffers
     */
    private static byte[] byteBuf;
    private static char[] charBuf;
    private static CharSequence charSeqBuf;
    private static short[] shortBuf;
    private static int[] intBuf;
    private static long[] longBuf;



	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		byteBuf = new byte[BUF_SIZE];
		charBuf = new char[BUF_SIZE / 2];
		shortBuf = new short[BUF_SIZE / 2];
		intBuf = new int[BUF_SIZE / 4];
		longBuf = new long[BUF_SIZE / 8];
		/*
		 * Fill byte buffer (as per TestResults in SpookyTestV2.cpp)
		 */
		for (int i = 0; i < BUF_SIZE; i++) {
			byteBuf[i] = (byte)(i + 128);			
		}
		
		/*
		 * Pack contents of byteBuf into longBuf, little-endian
		 */
		int iByte = 0;
		for (int i = 0; i < BUF_SIZE / 8; i++) {
			longBuf[i] = (long)byteBuf[iByte++] & 0xFFL;
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 8;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 16;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 24;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 32;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 40;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 48;			
			longBuf[i] |= ((long)byteBuf[iByte++] & 0xFFL) << 56;			
		}
		
		iByte = 0;
		for (int i = 0; i < BUF_SIZE / 4; i++) {
			intBuf[i] = (int)byteBuf[iByte++] & 0xFF;
			intBuf[i] |= ((int)byteBuf[iByte++] & 0xFF) << 8;			
			intBuf[i] |= ((int)byteBuf[iByte++] & 0xFF) << 16;			
			intBuf[i] |= ((int)byteBuf[iByte++] & 0xFF) << 24;			
		}
		
		iByte = 0;
		for (int i = 0; i < BUF_SIZE / 2; i++) {
			shortBuf[i] = (short)(byteBuf[iByte++] & 0xFF);
			shortBuf[i] |= (short)(byteBuf[iByte++] & 0xFF) << 8;			
		}
		
		for (int i = 0; i < BUF_SIZE / 2; i++) {
			charBuf[i] = (char)shortBuf[i];
		}

		charSeqBuf = new String(charBuf);
	}

	@Test
	public void testHashByte() {
		SpookyHashStream spooky = new SpookyHashStream();
		for (int i = 0; i < BUF_SIZE; i++) {
			spooky.initialize();
			spooky.put(byteBuf, 0, i);
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("buf %d: ", i), expected[i], lsaw);
		}
		for (int i = 0; i < BUF_SIZE; i++) {
			spooky.initialize();
			for (int j = 0; j < i; j++) {
				spooky.putByte(byteBuf[j]);
			}
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("per element %d: ", i), expected[i], lsaw);
		}		
		for (int i = 0; i < BUF_SIZE; i++) {
			for (int j = 1; j < i; j++) {
				spooky.initialize();
				spooky.put(byteBuf, 0, j).put(byteBuf, j, i - j);
				long[] result = spooky.hash();
				long lsaw = result[0] & RESULT_MASK;
				assertEquals(String.format("split %d size %d: ", j, i), expected[i], lsaw);
			}
		}
	}
	
	@Test
	public void testHashCharSequence() {
		SpookyHashStream spooky = new SpookyHashStream();
		for (int i = 0; i < BUF_SIZE/2; i++) {
			spooky.initialize();
			spooky.put(charSeqBuf.subSequence(0, i));
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("buf %d: ", i), expected[i*2], lsaw);
		}
		for (int i = 0; i < BUF_SIZE/2; i++) {
			spooky.initialize();
			for (int j = 0; j < i; j++) {
				spooky.putChar(charBuf[j]);
			}
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("per element %d: ", i), expected[i*2], lsaw);
		}		
		for (int i = 0; i < BUF_SIZE/2; i++) {
			for (int j = 1; j < i; j++) {
				spooky.initialize();
				long[] result = spooky.put(charSeqBuf.subSequence(0, j)).put(charSeqBuf.subSequence(j, i)).hash();
				long lsaw = result[0] & RESULT_MASK;
				assertEquals(String.format("split %d size %d: ", j, i), expected[i*2], lsaw);
			}
		}
	}

	@Test
	public void testHashShort() {
		SpookyHashStream spooky = new SpookyHashStream();
		for (int i = 0; i < BUF_SIZE/2; i++) {
			spooky.initialize();
			spooky.put(shortBuf, 0, i);
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("buf %d: ", i), expected[i*2], lsaw);
		}
		for (int i = 0; i < BUF_SIZE/2; i++) {
			spooky.initialize();
			for (int j = 0; j < i; j++) {
				spooky.putShort(shortBuf[j]);
			}
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("per element %d: ", i), expected[i*2], lsaw);
		}		
		for (int i = 0; i < BUF_SIZE/2; i++) {
			for (int j = 1; j < i; j++) {
				spooky.initialize();
				spooky.put(shortBuf, 0, j).put(shortBuf, j, i-j);
				long[] result = spooky.hash();
				long lsaw = result[0] & RESULT_MASK;
				assertEquals(String.format("split %d size %d: ", j, i), expected[i*2], lsaw);
			}
		}
	}
	
	@Test
	public void testHashInt() {
		SpookyHashStream spooky = new SpookyHashStream();
		for (int i = 0; i < BUF_SIZE/4; i++) {
			spooky.initialize();
			spooky.put(intBuf, 0, i);
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("buf %d: ", i), expected[i*4], lsaw);
		}
		for (int i = 0; i < BUF_SIZE/4; i++) {
			spooky.initialize();
			for (int j = 0; j < i; j++) {
				spooky.putInt(intBuf[j]);
			}
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("per element %d: ", i), expected[i*4], lsaw);
		}		
		for (int i = 0; i < BUF_SIZE/4; i++) {
			for (int j = 1; j < i; j++) {
				spooky.initialize();
				spooky.put(intBuf, 0, j).put(intBuf, j, i-j);
				long[] result = spooky.hash();
				long lsaw = result[0] & RESULT_MASK;
				assertEquals(String.format("split %d size %d: ", j, i), expected[i*4], lsaw);
			}
		}
	}
	
	@Test
	public void testHashLong() {
		SpookyHashStream spooky = new SpookyHashStream();
		for (int i = 0; i < BUF_SIZE/8; i++) {
			spooky.initialize();
			spooky.put(longBuf, 0, i);
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("buf %d: ", i), expected[i*8], lsaw);
		}
		for (int i = 0; i < BUF_SIZE/8; i++) {
			spooky.initialize();
			for (int j = 0; j < i; j++) {
				spooky.putLong(longBuf[j]);
			}
			long[] result = spooky.hash();
			long lsaw = result[0] & RESULT_MASK;
			assertEquals(String.format("per element %d: ", i), expected[i*8], lsaw);
		}		
		for (int i = 0; i < BUF_SIZE/8; i++) {
			for (int j = 1; j < i; j++) {
				spooky.initialize();
				spooky.put(longBuf, 0, j).put(longBuf, j, i-j);
				long[] result = spooky.hash();
				long lsaw = result[0] & RESULT_MASK;
				assertEquals(String.format("split %d size %d: ", j, i), expected[i*8], lsaw);
			}
		}
	}

	@Test
	public void testHashMixed() {
		SpookyHashStream spooky = new SpookyHashStream();
		int n = 0;
		long[] result;
		
		spooky.put(byteBuf, n, 2);  n += 2;
		assertEquals("mixed A byteCount", n, spooky.getByteCount());
		result = spooky.hash();
		assertEquals("mixed A result", expected[n], result[0] & RESULT_MASK);
		
		spooky.put(shortBuf, n/2, 1); n += 2;
		assertEquals("mixed B byteCount", n, spooky.getByteCount());
		result = spooky.hash();
		assertEquals("mixed B result", expected[n], result[0] & RESULT_MASK);			
	
		spooky.put(intBuf, n/4, 2); n += 8;
		assertEquals("mixed C byteCount", n, spooky.getByteCount());
		result = spooky.hash();
		assertEquals("mixed C result", expected[n], result[0] & RESULT_MASK);			
		
		spooky.put(charBuf, n/2, 1); n += 2;
		assertEquals("mixed D byteCount", n, spooky.getByteCount());
		result = spooky.hash();
		assertEquals("mixed D result", expected[n], result[0] & RESULT_MASK);			

		spooky.put(byteBuf, n, 2); n += 2;
		assertEquals("mixed E byteCount", n, spooky.getByteCount());
		result = spooky.hash();
		assertEquals("mixed E result", expected[n], result[0] & RESULT_MASK);			
	}
	
	@Test
	public void testAlignment() {
		SpookyHashStream s = new SpookyHashStream();
		long[] buf = s.getBuffer();
		s.putBoolean(true);
		assertEquals("alignment count A", 1, s.getByteCount());		
		assertEquals("alignment A", 1L, buf[0]);
		
		s.putChar('$');
		assertEquals("alignment count B", 4, s.getByteCount());		
		assertEquals("alignment B", 0x00240001L, buf[0]);

		s.putByte((byte)0x25);
		assertEquals("alignment count C", 5, s.getByteCount());		
		assertEquals("alignment C", 0x2500240001L, buf[0]);
		
		s.putInt(0x26);
		assertEquals("alignment count D", 12, s.getByteCount());		
		assertEquals("alignment D0", 0x2500240001L, buf[0]);
		assertEquals("alignment D1", 0x26L, buf[1]);

		s.putLong(0x27);
		assertEquals("alignment count E", 24, s.getByteCount());		
		assertEquals("alignment E0", 0x2500240001L, buf[0]);
		assertEquals("alignment E1", 0x26L, buf[1]);
		assertEquals("alignment E2", 0x27L, buf[2]);

	}

}
