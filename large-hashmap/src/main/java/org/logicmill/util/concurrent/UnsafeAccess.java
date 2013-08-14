package org.logicmill.util.concurrent;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/** 
 * Acquires a reference to the singleton instance of the Unsafe class,
 * circumventing the obstacles and safeguards put in place by the JRE/JDK.
 * 
 * @author David Curtis
 *
 */
@SuppressWarnings("restriction")
public class UnsafeAccess {

    private static Unsafe unsafe = null;
    
    static {
    try {
    	   Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
    	   field.setAccessible(true);
    	   unsafe = (sun.misc.Unsafe) field.get(null);
    	} catch (Exception e) {
    	   throw new AssertionError(e);
    	}
    }

    /** Returns a reference to the singleton instance of the Unsafe class.
     * @return a reference to the singleton instance of the Unsafe class
     */
    public static Unsafe getUnsafe() { return unsafe; }
	
}
