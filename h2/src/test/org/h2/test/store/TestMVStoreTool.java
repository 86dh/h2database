/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.StringWriter;
import java.util.Map.Entry;
import java.util.Random;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.Spatial;
import org.h2.mvstore.db.SpatialKey;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests the MVStoreTool class.
 */
public class TestMVStoreTool extends TestBase {

    public static final String BIG_STRING_WITH_C = new String(new char[3000]).replace("\0", "c");
    public static final String BIG_STRING_WITH_H = new String(new char[3000]).replace("\0", "H");


    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.config.big = true;
        test.testFromMain();
    }

    @Override
    public void test() throws Exception {
        testCompact();
        testDump();
        testRollback();
    }

    private void testCompact() {
        String fileName = getBaseDir() + "/testCompact.h3";
        String fileNameNew = fileName + ".new";
        String fileNameCompressed = fileNameNew + ".compress";

        FileUtils.createDirectories(getBaseDir());
        FileUtils.delete(fileName);
        // store with a very small page size, to make sure
        // there are many leaf pages
        MVStore s = new MVStore.Builder().
                pageSplitSize(1000).
                fileName(fileName).autoCommitDisabled().open();
        s.setRetentionTime(0);
        long start = System.currentTimeMillis();
        MVMap<Integer, String> map = s.openMap("data");
        int size = config.big ? 2_000_000 : 20_000;
        for (int i = 0; i < size; i++) {
            map.put(i, "Hello World " + i * 10);
            if (i % 10000 == 0) {
                s.commit();
            }
        }
        for (int i = 0; i < size; i += 2) {
            map.remove(i);
            if (i % 10000 == 0) {
                s.commit();
            }
        }
        for (int i = 0; i < 20; i++) {
            map = s.openMap("data" + i);
            for (int j = 0; j < i * i; j++) {
                map.put(j, "Hello World " + j * 10);
            }
            s.commit();
        }
        MVRTreeMap<String> rTreeMap = s.openMap("rtree", new MVRTreeMap.Builder<String>());
        Random r = new Random(1);
        for (int i = 0; i < 10; i++) {
            float x = r.nextFloat();
            float y = r.nextFloat();
            float width = r.nextFloat() / 10;
            float height = r.nextFloat() / 10;
            SpatialKey k = new SpatialKey(i, x, x + width, y, y + height);
            rTreeMap.put(k, "Hello World " + i * 10);
            if (i % 3 == 0) {
                s.commit();
            }
        }
        s.close();
        trace("Created in " + (System.currentTimeMillis() - start) + " ms.");

        start = System.currentTimeMillis();
        MVStoreTool.compact(fileName, fileNameNew, false);
        MVStoreTool.compact(fileName, fileNameCompressed, true);
        trace("Compacted in " + (System.currentTimeMillis() - start) + " ms.");
        long size1 = FileUtils.size(fileName);
        long size2 = FileUtils.size(fileNameNew);
        long size3 = FileUtils.size(fileNameCompressed);
        assertTrue("size1: " + size1 + " size2: " + size2 + " size3: " + size3,
                size2 < size1 && size3 < size2);

        start = System.currentTimeMillis();
        MVStoreTool.compact(fileNameNew, false);
        assertTrue(100L * Math.abs(size2 - FileUtils.size(fileNameNew)) / size2 < 1);
        MVStoreTool.compact(fileNameCompressed, true);
        assertEquals(size3, FileUtils.size(fileNameCompressed));
        trace("Re-compacted in " + (System.currentTimeMillis() - start) + " ms.");

        start = System.currentTimeMillis();
        MVStore s1 = new MVStore.Builder().
                fileName(fileName).readOnly().open();
        MVStore s2 = new MVStore.Builder().
                fileName(fileNameNew).readOnly().open();
        MVStore s3 = new MVStore.Builder().
                fileName(fileNameCompressed).readOnly().open();
        assertEquals(s1, s2);
        assertEquals(s1, s3);
        s1.close();
        s2.close();
        s3.close();
        trace("Verified in " + (System.currentTimeMillis() - start) + " ms.");
    }

    private void assertEquals(MVStore a, MVStore b) {
        assertEquals(a.getMapNames().size(), b.getMapNames().size());
        for (String mapName : a.getMapNames()) {
            if (mapName.startsWith("rtree")) {
                MVRTreeMap<String> ma = a.openMap(
                        mapName, new MVRTreeMap.Builder<String>());
                MVRTreeMap<String> mb = b.openMap(
                        mapName, new MVRTreeMap.Builder<String>());
                assertEquals(ma.sizeAsLong(), mb.sizeAsLong());
                for (Entry<Spatial, String> e : ma.entrySet()) {
                    Object x = mb.get(e.getKey());
                    assertEquals(e.getValue(), x.toString());
                }

            } else {
                MVMap<?, ?> ma = a.openMap(mapName);
                MVMap<?, ?> mb = a.openMap(mapName);
                assertEquals(ma.sizeAsLong(), mb.sizeAsLong());
                for (Entry<?, ?> e : ma.entrySet()) {
                    Object x = mb.get(e.getKey());
                    assertEquals(e.getValue().toString(), x.toString());
                }
            }
        }
    }

    private void testDump() {
        String fileName = getBaseDir() + "/testDump.h3";
        FileUtils.createDirectories(getBaseDir());
        FileUtils.delete(fileName);
        // store with a very small page size, to make sure
        // there are many leaf pages
        MVStore s = new MVStore.Builder().
                pageSplitSize(1000).
                fileName(fileName).autoCommitDisabled().open();
        s.setRetentionTime(0);
        MVMap<Integer, String> map = s.openMap("data");

        // Insert some data. Using big strings with "H" and "c" to validate the fix of #3931
        int nbEntries = 20_000;
        for (int i = 0; i < nbEntries; i++) {
            map.put(i, i % 2 == 0 ? BIG_STRING_WITH_C : BIG_STRING_WITH_H);
        }
        s.commit();
        // Let's rewrite the data to trigger some chunk compaction & drop
        for (int i = 0; i < nbEntries; i++) {
            map.put(i, i % 2 == 0 ? BIG_STRING_WITH_H : BIG_STRING_WITH_C);
        }
        s.commit();
        s.close();
        StringWriter dumpWriter = new StringWriter();
        MVStoreTool.dump(fileName, dumpWriter, true);

        int nbFileHeaders = nbOfOccurrences(dumpWriter.toString(), "fileHeader");
        assertEquals("Exactly 2 file headers are expected in the dump", 2, nbFileHeaders);
    }

    private void testRollback() {
        String fileName = getBaseDir() + "/testDump.h4";
        FileUtils.createDirectories(getBaseDir());
        FileUtils.delete(fileName);
        // store with a very small page size, to make sure
        // there are many leaf pages
        MVStore s = new MVStore.Builder().
                pageSplitSize(1000).
                fileName(fileName).autoCommitDisabled().open();
        s.setRetentionTime(0);
        MVMap<Integer, String> map = s.openMap("data");

        // Insert some data. Using big strings with "H" and "c" to validate the fix of #3931
        int nbEntries = 20_000;
        for (int i = 0; i < nbEntries; i++) {
            map.put(i, i % 2 == 0 ? BIG_STRING_WITH_C : BIG_STRING_WITH_H);
        }
        s.commit();
        // Let's rewrite the data to trigger some chunk compaction & drop
        for (int i = 0; i < nbEntries; i++) {
            map.put(i, i % 2 == 0 ? BIG_STRING_WITH_H : BIG_STRING_WITH_C);
        }
        s.commit();
        s.close();
        StringWriter dumpWriter = new StringWriter();
        try {
            MVStoreTool.rollback(fileName, Long.MAX_VALUE, dumpWriter);
        } catch (NullPointerException ex ) {
            fail("No NullPointerException expected");
        }
    }

    private static int nbOfOccurrences(String str, String pattern) {
        return str.split(pattern,-1).length - 1;
    }

}
