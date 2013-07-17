package org.logicmill.util;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

class ReflectionProbe {
	
	/*
	 * Static methods to extract fields of various types from an object
	 */

	static Object getObjectField(Object obj, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.get(obj);
	}
	
	static int getIntField(Object obj, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getInt(obj);
	}
	
	static boolean getBooleanField(Object obj, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getBoolean(obj);
	}
	
	static long getLongField(Object obj, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getLong(obj);
	}
	
	static AtomicInteger getAtomicIntegerField(Object obj, String fieldName) 
			throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		Object intObj = field.get(obj);
		if (intObj instanceof AtomicInteger) {
			return (AtomicInteger) intObj;
		} else {
			throw new IllegalArgumentException();
		}
		
	}
	
	static AtomicIntegerArray getAtomicIntegerArrayField(Object obj, String fieldName) 
	throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		Object arrayObj = field.get(obj);
		if (arrayObj instanceof AtomicIntegerArray) {
			return (AtomicIntegerArray) arrayObj;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	@SuppressWarnings("rawtypes")
	static AtomicReferenceArray getAtomicReferenceArrayField(Object obj, String fieldName) 
	throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		Object arrayObj = field.get(obj);
		if (arrayObj instanceof AtomicReferenceArray) {
			return (AtomicReferenceArray) arrayObj;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	@SuppressWarnings("rawtypes")
	static AtomicReference getAtomicReferenceField(Object obj, String fieldName)
	throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field field = obj.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		Object refObj = field.get(obj);
		if (refObj instanceof AtomicReference) {
			return (AtomicReference) refObj;
		} else {
			throw new IllegalArgumentException();
		}
		
	}

}
