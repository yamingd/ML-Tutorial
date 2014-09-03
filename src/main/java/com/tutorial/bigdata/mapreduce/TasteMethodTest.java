package com.tutorial.bigdata.mapreduce;

import com.google.common.primitives.Longs;

/**
 * Created by yaming_deng on 14-4-11.
 */
public class TasteMethodTest {

    /**
     * Maps a long to an int with range of 0 to Integer.MAX_VALUE-1
     */
    public static int idToIndex(long id) {
        return 0x7FFFFFFF & Longs.hashCode(id) % 0x7FFFFFFE;
    }

    public static void main(String[] args){
        System.out.println(idToIndex(1L));
        System.out.println(idToIndex(100L));
        System.out.println(idToIndex(1000L));
        System.out.println(idToIndex(10000L));
        System.out.println(idToIndex(100000L));
        System.out.println(idToIndex(Integer.MAX_VALUE));
        System.out.println(idToIndex(Long.MAX_VALUE));
    }
}
