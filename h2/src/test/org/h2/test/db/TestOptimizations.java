/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Task;

/**
 * Test various optimizations (query cache, optimization for MIN(..), and
 * MAX(..)).
 */
public class TestOptimizations extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("optimizations");
        testConditionsStackOverflow();
        testIdentityIndexUsage();
        testFastRowIdCondition();
        testExplainRoundTrip();
        testOrderByExpression();
        testGroupSubquery();
        testAnalyzeLob();
        testLike();
        testExistsSubquery();
        testQueryCacheConcurrentUse();
        testQueryCacheResetParams();
        testRowId();
        testSortIndex();
        testAutoAnalyze();
        testInAndBetween();
        testNestedIn();
        testConstantIn1();
        testConstantIn2();
        testConstantTypeConversionToColumnType();
        testNestedInSelectAndLike();
        testNestedInSelect();
        testInSelectJoin();
        testMinMaxNullOptimization();
        testUseCoveringIndex();
        testInPredicate();
        // testUseIndexWhenAllColumnsNotInOrderBy();
        if (config.networked) {
            return;
        }
        testOptimizeInJoinSelect();
        testOptimizeInJoin();
        testMultiColumnRangeQuery();
        testDistinctOptimization();
        testQueryCacheTimestamp();
        if (!config.lazy) {
            testQueryCacheSpeed();
        }
        testQueryCache(true);
        testQueryCache(false);
        testIn();
        testMinMaxCountOptimization(true);
        testMinMaxCountOptimization(false);
        testOrderedIndexes();
        testIndexUseDespiteNullsFirst();
        testConvertOrToIn();
        testConditionAndOrDistributiveLaw();
        deleteDb("optimizations");
    }

    private void testIdentityIndexUsage() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a identity)");
        stat.execute("insert into test values()");
        ResultSet rs = stat.executeQuery("explain select * from test where a = 1");
        rs.next();
        assertContains(rs.getString(1), "PRIMARY_KEY");
        stat.execute("drop table test");
        conn.close();
    }

    private void testFastRowIdCondition() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.executeUpdate("create table many(id int) " +
                "as select x from system_range(1, 10000)");
        ResultSet rs = stat.executeQuery("explain analyze select * from many " +
                "where _rowid_ = 400");
        rs.next();
        assertContains(rs.getString(1), "/* scanCount: 2 */");
        conn.close();
    }

    private void testExplainRoundTrip() throws Exception {
        Connection conn = getConnection("optimizations");
        assertExplainRoundTrip(conn, "SELECT \"X\" FROM SYSTEM_RANGE(1, 1)"
                + " WHERE \"X\" > ANY(SELECT DISTINCT \"X\" FROM SYSTEM_RANGE(1, 1))");
        conn.close();
    }

    private void assertExplainRoundTrip(Connection conn, String sql)
            throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain " + sql);
        rs.next();
        String plan = rs.getString(1);
        plan = plan.replaceAll("\\s+", " ");
        plan = plan.replaceAll("/\\*[^\\*]*\\*/", "");
        plan = plan.replaceAll("\\s+", " ");
        plan = plan.replaceAll("\\( ", "\\(");
        plan = plan.replaceAll(" \\)", "\\)");
        assertEquals(sql, plan);
    }

    private void testOrderByExpression() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'Hello'), (3, 'Hello')");
        ResultSet rs;
        rs = stat.executeQuery(
                "explain select name from test where name='Hello' order by name");
        rs.next();
        String plan = rs.getString(1);
        assertContains(plan, "tableScan");
        stat.execute("drop table test");
        conn.close();
    }

    private void testGroupSubquery() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table t1(id int)");
        stat.execute("create table t2(id int)");
        stat.execute("insert into t1 values(2), (2), (3)");
        stat.execute("insert into t2 values(2), (3)");
        stat.execute("create index t1id_index on t1(id)");
        ResultSet rs;
        rs = stat.executeQuery("select id, (select count(*) from t2 " +
                "where t2.id = t1.id) cc from t1 group by id order by id");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        stat.execute("drop table t1, t2");
        conn.close();
    }

    private void testAnalyzeLob() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(v varchar, b varbinary, cl clob, bl blob) as " +
                "select ' ', '00', ' ', '00' from system_range(1, 100)");
        stat.execute("analyze");
        ResultSet rs = stat.executeQuery("select column_name, selectivity " +
                "from information_schema.columns where table_name='TEST'");
        rs.next();
        assertEquals("V", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals("B", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals("CL", rs.getString(1));
        assertEquals(50, rs.getInt(2));
        rs.next();
        assertEquals("BL", rs.getString(1));
        assertEquals(50, rs.getInt(2));
        stat.execute("drop table test");
        conn.close();
    }

    private void testLike() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(name varchar primary key) as " +
                "select x from system_range(1, 10)");
        ResultSet rs = stat.executeQuery("explain select * from test " +
                "where name like ? || '%' {1: 'Hello'}");
        rs.next();
        // ensure the ID = 10 part is evaluated first
        assertContains(rs.getString(1), "PRIMARY_KEY_");
        stat.execute("drop table test");
        conn.close();
    }

    private void testExistsSubquery() throws Exception {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int) as select x from system_range(1, 10)");
        ResultSet rs = stat.executeQuery("explain select * from test " +
                "where exists(select 1 from test, test, test) and id = 10");
        rs.next();
        // ensure the ID = 10 part is evaluated first
        assertContains(rs.getString(1), "WHERE (\"ID\" = 10)");
        stat.execute("drop table test");
        conn.close();
    }

    private void testQueryCacheConcurrentUse() throws Exception {
        if (config.lazy) {
            return;
        }
        final Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, data clob)");
        stat.execute("insert into test values(0, space(10000))");
        stat.execute("insert into test values(1, space(10001))");
        Task[] tasks = new Task[2];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    PreparedStatement prep = conn.prepareStatement(
                            "select * from test where id = ?");
                    while (!stop) {
                        int x = (int) (Math.random() * 2);
                        prep.setInt(1, x);
                        ResultSet rs = prep.executeQuery();
                        rs.next();
                        String data = rs.getString(2);
                        if (data.length() != 10000 + x) {
                            throw new Exception(data.length() + " != " + x);
                        }
                        rs.close();
                    }
                }
            };
            tasks[i].execute();
        }
        Thread.sleep(1000);
        for (Task t : tasks) {
            t.get();
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testQueryCacheResetParams() throws SQLException {
        Connection conn = getConnection("optimizations");
        PreparedStatement prep;
        prep = conn.prepareStatement("select ?");
        prep.setString(1, "Hello");
        prep.execute();
        prep.close();
        prep = conn.prepareStatement("select ?");
        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, prep).execute();
        prep.close();
        conn.close();
    }

    private void testRowId() throws SQLException {
        if (config.memory) {
            return;
        }
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table test(data varchar)");
        stat.execute("select min(_rowid_ + 1) from test");
        stat.execute("insert into test(_rowid_, data) values(10, 'Hello')");
        stat.execute("insert into test(data) values('World')");
        stat.execute("insert into test(_rowid_, data) values(20, 'Hello')");
        stat.execute(
                "merge into test using (values(20, 'Hallo')) s(id, data) on test._rowid_ = s.id"
                + " when matched then update set data = s.data");
        rs = stat.executeQuery(
                "select _rowid_, data from test order by _rowid_");
        rs.next();
        assertEquals(10, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(11, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        rs.next();
        assertEquals(20, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(0, 'Hello')");
        stat.execute("insert into test values(3, 'Hello')");
        stat.execute("insert into test values(2, 'Hello')");

        rs = stat.executeQuery("explain select * from test where _rowid_ = 2");
        rs.next();
        assertContains(rs.getString(1), ".tableScan: _ROWID_ =");

        rs = stat.executeQuery("explain select * from test where _rowid_ > 2");
        rs.next();
        assertContains(rs.getString(1), ".tableScan: _ROWID_ >");

        rs = stat.executeQuery("explain select * from test order by _rowid_");
        rs.next();
        assertContains(rs.getString(1), "/* index sorted */");
        rs = stat.executeQuery("select _rowid_, * from test order by _rowid_");
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(3, rs.getInt(2));

        stat.execute("drop table test");
        conn.close();
    }

    private void testSortIndex() throws SQLException {
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("drop table test if exists");
        stat.execute("create table test(id int)");
        stat.execute("create index idx_id_asc on test(id)");
        ResultSet rs;

        rs = stat.executeQuery("explain select * from test " +
                "where id > 10 order by id");
        rs.next();
        assertContains(rs.getString(1), "IDX_ID_ASC");

        rs = stat.executeQuery("explain select * from test " +
                "where id < 10 order by id desc");
        rs.next();
        assertContains(rs.getString(1), "IDX_ID_ASC");

        rs.next();
        stat.execute("drop table test");
        conn.close();
    }

    private void testAutoAnalyze() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'analyzeAuto'");
        int auto = rs.next() ? rs.getInt(1) : 0;
        if (auto != 0) {
            stat.execute("create table test(id int)");
            stat.execute("create user onlyInsert password ''");
            stat.execute("grant insert on test to onlyInsert");
            Connection conn2 = getConnection(
                    "optimizations", "onlyInsert", getPassword(""));
            Statement stat2 = conn2.createStatement();
            stat2.execute("insert into test select x " +
                    "from system_range(1, " + (auto + 10) + ")");
            conn2.close();
        }
        conn.close();
    }

    private void testInAndBetween() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create table test(id int, name varchar)");
        stat.execute("create index idx_name on test(id, name)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        rs = stat.executeQuery("select * from test " +
                "where id between 1 and 3 and name in ('World')");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from test " +
                "where id between 1 and 3 and name in (select 'World')");
        assertTrue(rs.next());
        stat.execute("drop table test");
        conn.close();
    }

    private void testNestedIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table accounts(id integer primary key, " +
                "status varchar(255), tag varchar(255))");
        stat.execute("insert into accounts values (31, 'X', 'A')");
        stat.execute("create table parent(id int)");
        stat.execute("insert into parent values(31)");
        stat.execute("create view test_view as select a.status, a.tag " +
                "from accounts a, parent t where a.id = t.id");
        rs = stat.executeQuery("select * from test_view " +
                "where status='X' and tag in ('A','B')");
        assertTrue(rs.next());
        rs = stat.executeQuery("select * from (select a.status, a.tag " +
                "from accounts a, parent t where a.id = t.id) x " +
                "where status='X' and tag in ('A','B')");
        assertTrue(rs.next());

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("create unique index idx_name on test(name, id)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        rs = stat.executeQuery("select * from (select * from test) " +
                "where id=1 and name in('Hello', 'World')");
        assertTrue(rs.next());
        stat.execute("drop table test");

        conn.close();
    }

    private void testConstantIn1() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        assertSingleValue(stat,
                "select count(*) from test where name in ('Hello', 'World', '1')", 2);
        assertSingleValue(stat,
                "select count(*) from test where name in ('Hello', 'World')", 2);
        assertSingleValue(stat,
                "select count(*) from test where name in ('Hello', 'Not')", 1);
        stat.execute("drop table test");

        conn.close();
    }

    private void testConstantIn2() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations;IGNORECASE=TRUE");
        Statement stat = conn.createStatement();

        stat.executeUpdate("CREATE TABLE testValues (x VARCHAR(50))");
        stat.executeUpdate("INSERT INTO testValues (x) SELECT 'foo' x");
        ResultSet resultSet;
        resultSet = stat.executeQuery(
                "SELECT x FROM testValues WHERE x IN ('foo')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery(
                "SELECT x FROM testValues WHERE x IN ('FOO')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery(
                "SELECT x FROM testValues WHERE x IN ('foo','bar')");
        assertTrue(resultSet.next());
        resultSet = stat.executeQuery(
                "SELECT x FROM testValues WHERE x IN ('FOO','bar')");
        assertTrue(resultSet.next());

        conn.close();
    }

    private void testConstantTypeConversionToColumnType() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations;IGNORECASE=TRUE");
        Statement stat = conn.createStatement();

        stat.executeUpdate("CREATE TABLE test (x int)");
        ResultSet resultSet;
        resultSet = stat.executeQuery(
            "EXPLAIN SELECT x FROM test WHERE x = '5'");

        assertTrue(resultSet.next());
        // String constant '5' has been converted to int constant 5 on
        // optimization
        assertTrue(resultSet.getString(1).endsWith("\"X\" = 5"));

        stat.execute("drop table test");

        conn.close();
    }

    private void testNestedInSelect() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create table test(id int primary key, name varchar) " +
                "as select 1, 'Hello'");
        stat.execute("select * from (select * from test) " +
                "where id=1 and name in('Hello', 'World')");

        stat.execute("drop table test");

        stat.execute("create table test(id int, name varchar) as select 1, 'Hello'");
        stat.execute("create index idx2 on test(id, name)");
        rs = stat.executeQuery("select count(*) from test " +
                "where id=1 and name in('Hello', 'x')");
        rs.next();
        assertEquals(1, rs.getInt(1));

        conn.close();
    }

    private void testNestedInSelectAndLike() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(2)");
        ResultSet rs = stat.executeQuery("select * from test where id in(1, 2)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        stat.execute("create table test2(id int primary key hash)");
        stat.execute("insert into test2 values(2)");
        rs = stat.executeQuery("select * from test where id in(1, 2)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        PreparedStatement prep;
        prep = conn.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 1) A " +
                "WHERE A.X IN (SELECT B.X FROM SYSTEM_RANGE(1, 1) B WHERE B.X LIKE ?)");
        prep.setString(1, "1");
        prep.execute();
        prep = conn.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 1) A " +
                "WHERE A.X IN (SELECT B.X FROM SYSTEM_RANGE(1, 1) B WHERE B.X IN (?, ?))");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.executeQuery();
        conn.close();
    }

    private void testInSelectJoin() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int, c int, d int) " +
                "as select 1, 1, 1, 1 from dual;");
        ResultSet rs;
        PreparedStatement prep;
        prep = conn.prepareStatement("SELECT 2 FROM TEST A "
                + "INNER JOIN (SELECT DISTINCT B.C AS X FROM TEST B "
                + "WHERE B.D = ?2) V ON 1=1 WHERE (A = ?1) AND (B = V.X)");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());

        prep = conn.prepareStatement(
                "select 2 from test a where a=? and b in(" +
                "select b.c from test b where b.d=?)");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        conn.close();
    }


    private void testOptimizeInJoinSelect() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table item(id int primary key)");
        stat.execute("insert into item values(1)");
        stat.execute("create alias opt for '" + getClass().getName() + ".optimizeInJoinSelect'");
        PreparedStatement prep = conn.prepareStatement(
                "select * from item where id in (select x from opt())");
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return a result set
     */
    public static ResultSet optimizeInJoinSelect() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", Types.INTEGER, 0, 0);
        rs.addRow(1);
        return rs;
    }

    private void testOptimizeInJoin() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test select x from system_range(1, 1000)");
        ResultSet rs = stat.executeQuery("explain select * " +
                "from test where id in (400, 300)");
        rs.next();
        String plan = rs.getString(1);
        if (plan.indexOf("/* PUBLIC.PRIMARY_KEY_") < 0) {
            fail("Expected using the primary key, got: " + plan);
        }
        conn.close();
    }

    private void testMinMaxNullOptimization() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        Random random = new Random(1);
        int len = getSize(50, 500);
        for (int i = 0; i < len; i++) {
            stat.execute("drop table if exists test");
            stat.execute("create table test(x int)");
            if (random.nextBoolean()) {
                int count = random.nextBoolean() ? 1 : 1 + random.nextInt(len);
                if (count > 0) {
                    stat.execute("insert into test select null " +
                            "from system_range(1, " + count + ")");
                }
            }
            int maxExpected = -1;
            int minExpected = -1;
            if (random.nextInt(10) != 1) {
                minExpected = 1;
                maxExpected = 1 + random.nextInt(len);
                stat.execute("insert into test select x " +
                        "from system_range(1, " + maxExpected + ")");
            }
            String sql = "create index idx on test(x";
            if (random.nextBoolean()) {
                sql += " desc";
            }
            if (random.nextBoolean()) {
                if (random.nextBoolean()) {
                    sql += " nulls first";
                } else {
                    sql += " nulls last";
                }
            }
            sql += ")";
            stat.execute(sql);
            ResultSet rs = stat.executeQuery(
                    "explain select min(x), max(x) from test");
            rs.next();
            rs = stat.executeQuery("select min(x), max(x) from test");
            rs.next();
            int min = rs.getInt(1);
            if (rs.wasNull()) {
                min = -1;
            }
            int max = rs.getInt(2);
            if (rs.wasNull()) {
                max = -1;
            }
            assertEquals(minExpected, min);
            assertEquals(maxExpected, max);
        }
        conn.close();
    }

    private void testMultiColumnRangeQuery() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Logs(id INT PRIMARY KEY, type INT)");
        stat.execute("CREATE unique INDEX type_index ON Logs(type, id)");
        stat.execute("INSERT INTO Logs SELECT X, MOD(X, 3) " +
                "FROM SYSTEM_RANGE(1, 1000)");
        stat.execute("ANALYZE SAMPLE_SIZE 0");
        ResultSet rs;
        rs = stat.executeQuery("EXPLAIN SELECT id FROM Logs " +
                "WHERE id < 100 and type=2 AND id<100");
        rs.next();
        String plan = rs.getString(1);
        assertContains(plan, "TYPE_INDEX");
        conn.close();
    }

    private void testUseIndexWhenAllColumnsNotInOrderBy() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, account int, tx int)");
        stat.execute("insert into test select x, x*100, x from system_range(1, 10000)");
        stat.execute("analyze sample_size 5");
        stat.execute("create unique index idx_test_account_tx on test(account, tx desc)");
        ResultSet rs;
        rs = stat.executeQuery("explain analyze " +
                "select tx from test " +
                "where account=22 and tx<9999999 " +
                "order by tx desc limit 25");
        rs.next();
        String plan = rs.getString(1);
        assertContains(plan, "index sorted");
        conn.close();
    }

    private void testDistinctOptimization() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "NAME VARCHAR, TYPE INT)");
        stat.execute("CREATE INDEX IDX_TEST_TYPE ON TEST(TYPE)");
        Random random = new Random(1);
        int len = getSize(10000, 100000);
        int[] groupCount = new int[10];
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?)");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Hello World");
            int type = random.nextInt(10);
            groupCount[type]++;
            prep.setInt(3, type);
            prep.execute();
        }
        ResultSet rs;
        rs = stat.executeQuery("SELECT TYPE, COUNT(*) FROM TEST " +
                "GROUP BY TYPE ORDER BY TYPE");
        for (int i = 0; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
            assertEquals(groupCount[i], rs.getInt(2));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST " +
                "ORDER BY TYPE");
        for (int i = 0; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST");
        for (int i = 0; rs.next(); i++) {
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        stat.execute("ANALYZE");
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST " +
                "ORDER BY TYPE");
        for (int i = 0; i < 10; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        rs = stat.executeQuery("SELECT DISTINCT TYPE FROM TEST " +
                "ORDER BY TYPE LIMIT 5 OFFSET 2");
        for (int i = 2; i < 7; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        conn.close();
    }

    private void testQueryCacheTimestamp() throws Exception {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT CURRENT_TIMESTAMP()");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String a = rs.getString(1);
        Thread.sleep(50);
        rs = prep.executeQuery();
        rs.next();
        String b = rs.getString(1);
        assertFalse(a.equals(b));
        conn.close();
    }

    private void testQueryCacheSpeed() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        testQuerySpeed(stat,
                "select sum(a.n), sum(b.x) from system_range(1, 100) b, " +
                "(select sum(x) n from system_range(1, 4000)) a");
        conn.close();
    }

    private void testQuerySpeed(Statement stat, String sql) throws SQLException {
        long totalTime = 0;
        long totalTimeOptimized = 0;
        for (int i = 0; i < 3; i++) {
            totalTime += measureQuerySpeed(stat, sql, false);
            totalTimeOptimized += measureQuerySpeed(stat, sql, true);
        }
        // System.out.println(
        //         TimeUnit.NANOSECONDS.toMillis(totalTime) + " " +
        //         TimeUnit.NANOSECONDS.toMillis(totalTimeOptimized));
        if (totalTimeOptimized > totalTime) {
            fail("not optimized: " + TimeUnit.NANOSECONDS.toMillis(totalTime) +
                    " optimized: " + TimeUnit.NANOSECONDS.toMillis(totalTimeOptimized) +
                    " sql:" + sql);
        }
    }

    private static long measureQuerySpeed(Statement stat, String sql, boolean optimized) throws SQLException {
        stat.execute("set OPTIMIZE_REUSE_RESULTS " + (optimized ? "1" : "0"));
        stat.execute(sql);
        long time = System.nanoTime();
        stat.execute(sql);
        return System.nanoTime() - time;
    }

    private void testQueryCache(boolean optimize) throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        if (optimize) {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 1");
        } else {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 0");
        }
        stat.execute("create table test(id int)");
        stat.execute("create table test2(id int)");
        stat.execute("insert into test values(1), (1), (2)");
        stat.execute("insert into test2 values(1)");
        PreparedStatement prep = conn.prepareStatement(
                    "select * from test where id = (select id from test2)");
        ResultSet rs1 = prep.executeQuery();
        rs1.next();
        assertEquals(1, rs1.getInt(1));
        rs1.next();
        assertEquals(1, rs1.getInt(1));
        assertFalse(rs1.next());

        stat.execute("update test2 set id = 2");
        ResultSet rs2 = prep.executeQuery();
        rs2.next();
        assertEquals(2, rs2.getInt(1));

        conn.close();
    }

    private void testMinMaxCountOptimization(boolean memory)
            throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create " + (memory ? "memory" : "") +
                " table test(id int primary key, v int)");
        stat.execute("create index idx_v_id on test(v, id);");
        int len = getSize(1000, 10000);
        HashMap<Integer, Integer> map = new HashMap<>();
        TreeSet<Integer> set = new TreeSet<>();
        Random random = new Random(1);
        for (int i = 0; i < len; i++) {
            if (i == len / 2) {
                if (!config.memory) {
                    conn.close();
                    conn = getConnection("optimizations");
                    stat = conn.createStatement();
                }
            }
            switch (random.nextInt(10)) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
                if (random.nextInt(1000) == 1) {
                    stat.execute("insert into test values(" + i + ", null)");
                    map.put(i, null);
                } else {
                    int value = random.nextInt();
                    stat.execute("insert into test values(" + i + ", " + value + ")");
                    map.put(i, value);
                    set.add(value);
                }
                break;
            case 6:
            case 7:
            case 8: {
                if (map.size() > 0) {
                    for (int j = random.nextInt(i), k = 0; k < 10; k++, j++) {
                        if (map.containsKey(j)) {
                            Integer x = map.remove(j);
                            if (x != null) {
                                set.remove(x);
                            }
                            stat.execute("delete from test where id=" + j);
                        }
                    }
                }
                break;
            }
            case 9: {
                ArrayList<Integer> list = new ArrayList<>(map.values());
                int count = list.size();
                Integer min = null, max = null;
                if (count > 0) {
                    min = set.first();
                    max = set.last();
                }
                ResultSet rs = stat.executeQuery(
                        "select min(v), max(v), count(*) from test");
                rs.next();
                Integer minDb = (Integer) rs.getObject(1);
                Integer maxDb = (Integer) rs.getObject(2);
                int countDb = rs.getInt(3);
                assertEquals(minDb, min);
                assertEquals(maxDb, max);
                assertEquals(countDb, count);
                break;
            }
            default:
            }
        }
        conn.close();
    }

    private void testIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;

        assertFalse(stat.executeQuery("select * from dual " +
                "where x in()").next());
        assertFalse(stat.executeQuery("select * from dual " +
                "where null in(1)").next());
        assertFalse(stat.executeQuery("select * from dual " +
                "where null in(null)").next());
        assertFalse(stat.executeQuery("select * from dual " +
                "where null in(null, 1)").next());

        assertFalse(stat.executeQuery("select * from system_range(1, 1) " +
                "where 1+x in(3, 4)").next());
        assertFalse(stat.executeQuery("select * from system_range(1, 1) d1, dual d2 " +
                "where d1.x in(3, 4)").next());

        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("insert into test values(2, 'World')");

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?)");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 " +
                "where t1.id in(?, ?) order by id");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id=?)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id=? and t1.id<>t2.id)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id "
                + "in(select t2.id from test t2 where t2.id in(cast(?+10 as varchar)))");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        conn.close();
    }

    /**
     * Where there are multiple indices, and we have an ORDER BY, select the
     * index that already has the required ordering.
     */
    private void testOrderedIndexes() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE my_table(K1 INT, K2 INT, " +
                "VAL VARCHAR, PRIMARY KEY(K1, K2))");
        stat.execute("CREATE INDEX my_index ON my_table(K1, VAL)");
        ResultSet rs = stat.executeQuery(
                "EXPLAIN PLAN FOR SELECT * FROM my_table WHERE K1=7 " +
                "ORDER BY K1, VAL");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.MY_INDEX: K1 = 7 */");

        stat.execute("DROP TABLE my_table");

        // where we have two covering indexes, make sure
        // we choose the one that covers more
        stat.execute("CREATE TABLE my_table(K1 INT, K2 INT, VAL VARCHAR)");
        stat.execute("CREATE INDEX my_index1 ON my_table(K1, K2)");
        stat.execute("CREATE INDEX my_index2 ON my_table(K1, K2, VAL)");
        rs = stat.executeQuery(
                "EXPLAIN PLAN FOR SELECT * FROM my_table WHERE K1=7 " +
                "ORDER BY K1, K2, VAL");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.MY_INDEX2: K1 = 7 */");

        conn.close();
    }

    private void testIndexUseDespiteNullsFirst() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE my_table(K1 INT)");
        stat.execute("CREATE INDEX my_index ON my_table(K1)");
        stat.execute("INSERT INTO my_table VALUES (NULL)");
        stat.execute("INSERT INTO my_table VALUES (1)");
        stat.execute("INSERT INTO my_table VALUES (2)");

        ResultSet rs;
        String result;


        rs = stat.executeQuery(
            "EXPLAIN PLAN FOR SELECT * FROM my_table " +
                "ORDER BY K1 ASC NULLS FIRST");
        rs.next();
        result = rs.getString(1);
        assertContains(result, "/* index sorted */");

        rs = stat.executeQuery(
            "SELECT * FROM my_table " +
                "ORDER BY K1 ASC NULLS FIRST");
        rs.next();
        assertNull(rs.getObject(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));

        // ===
        rs = stat.executeQuery(
            "EXPLAIN PLAN FOR SELECT * FROM my_table " +
                "ORDER BY K1 DESC NULLS FIRST");
        rs.next();
        result = rs.getString(1);
        if (result.contains("/* index sorted */")) {
            fail(result + " does not contain: /* index sorted */");
        }

        rs = stat.executeQuery(
            "SELECT * FROM my_table " +
                "ORDER BY K1 DESC NULLS FIRST");
        rs.next();
        assertNull(rs.getObject(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));

        // ===
        rs = stat.executeQuery(
            "EXPLAIN PLAN FOR SELECT * FROM my_table " +
                "ORDER BY K1 ASC NULLS LAST");
        rs.next();
        result = rs.getString(1);
        if (result.contains("/* index sorted */")) {
            fail(result + " does not contain: /* index sorted */");
        }

        rs = stat.executeQuery(
            "SELECT * FROM my_table " +
                "ORDER BY K1 ASC NULLS LAST");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertNull(rs.getObject(1));

        // TODO: Test "EXPLAIN PLAN FOR SELECT * FROM my_table ORDER BY K1 DESC NULLS FIRST"
        // Currently fails, as using the index when sorting DESC is currently not supported.

        stat.execute("DROP TABLE my_table");
        conn.close();
    }

    private void testConvertOrToIn() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("insert into test values" +
                "(1, '1'), (2, '2'), (3, '3'), (4, '4'), (5, '5')");

        ResultSet rs = stat.executeQuery("EXPLAIN PLAN FOR SELECT * " +
                "FROM test WHERE ID=1 OR ID=2 OR ID=3 OR ID=4 OR ID=5");
        rs.next();
        assertContains(rs.getString(1), "\"ID\" IN(1, 2, 3, 4, 5)");

        rs = stat.executeQuery("SELECT COUNT(*) FROM test " +
                "WHERE ID=1 OR ID=2 OR ID=3 OR ID=4 OR ID=5");
        rs.next();
        assertEquals(5, rs.getInt(1));

        conn.close();
    }

    private void testUseCoveringIndex() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TABLE_A(id IDENTITY PRIMARY KEY NOT NULL, " +
                "name VARCHAR NOT NULL, active BOOLEAN DEFAULT TRUE, " +
                "CONSTRAINT TABLE_A_UK UNIQUE (name) )");
        stat.execute("CREATE TABLE TABLE_B(id IDENTITY PRIMARY KEY NOT NULL,  " +
                "TABLE_a_id BIGINT NOT NULL,  createDate TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, " +
                "CONSTRAINT TABLE_B_UK UNIQUE (table_a_id, createDate))");
        stat.execute("CREATE INDEX TABLE_B_IDX ON TABLE_B(TABLE_A_ID)");
        stat.execute("ALTER TABLE TABLE_B ADD FOREIGN KEY (table_a_id) REFERENCES TABLE_A(id)");
        stat.execute("INSERT INTO TABLE_A (name)  SELECT 'package_' || CAST(X as VARCHAR) " +
                "FROM SYSTEM_RANGE(1, 100)  WHERE X <= 100");
        int count = config.memory ? 30_000 : 50_000;
        stat.execute("INSERT INTO TABLE_B (table_a_id, createDate)  SELECT " +
                "CASE WHEN table_a_id = 0 THEN 1 ELSE table_a_id END, createDate " +
                "FROM ( SELECT ROUND((RAND() * 100)) AS table_a_id, " +
                "DATEADD('SECOND', X, CURRENT_TIMESTAMP) as createDate FROM SYSTEM_RANGE(1, " + count + ") " +
                "WHERE X < " + count + "  )");
        stat.execute("ANALYZE");

        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT MAX(b.id) as id " +
                "FROM table_b b JOIN table_a a ON b.table_a_id = a.id GROUP BY b.table_a_id " +
                "HAVING A.ACTIVE = TRUE");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.TABLE_B_IDX: TABLE_A_ID = A.ID */");

        rs = stat.executeQuery("EXPLAIN ANALYZE SELECT MAX(id) FROM table_b GROUP BY table_a_id");
        rs.next();
        assertContains(rs.getString(1), "/* PUBLIC.TABLE_B_IDX");
        conn.close();
    }

    private void testInPredicate() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Q_T(A INT) AS VALUES 1, 3, 4, 2");
        stat.execute("CREATE TABLE Q_T_A(A INT, PRIMARY KEY(A ASC)) AS VALUES 1, 3, 4, 2");
        stat.execute("CREATE TABLE Q_T_D(A INT, PRIMARY KEY(A DESC)) AS VALUES 1, 3, 4, 2");
        stat.execute("CREATE TABLE V_T(V INT) AS VALUES 2, 1, 5, 4");
        stat.execute("CREATE TABLE V_T_A(V INT, PRIMARY KEY(V ASC)) AS VALUES 2, 1, 5, 4");
        stat.execute("CREATE TABLE V_T_D(V INT, PRIMARY KEY(V DESC)) AS VALUES 2, 1, 5, 4");
        for (int q = 1; q <= 3; q++) {
            for (int qOrder = 1; qOrder <= 3; qOrder++) {
                testInPredicate(conn, stat, q, qOrder, 0, 1);
                for (int v = 1; v <= 3; v++) {
                    for (int vOrder = 1; vOrder <= 3; vOrder++) {
                        testInPredicate(conn, stat, q, qOrder, v, vOrder);
                    }
                }
            }
        }
        conn.close();
    }

    private void testInPredicate(Connection conn, Statement stat, int q, int qOrder, int v, int vOrder)
            throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ");
        switch (q) {
        case 1:
            builder.append("Q_T");
            break;
        case 2:
            builder.append("Q_T_A");
            break;
        case 3:
            builder.append("Q_T_D");
            break;
        default:
            fail();
        }
        builder.append(" WHERE A IN (");
        switch (v) {
        case 0:
            builder.append("2, 1, 5, 4");
            break;
        case 1:
            builder.append("SELECT * FROM V_T");
            break;
        case 2:
            builder.append("SELECT * FROM V_T");
            break;
        case 3:
            builder.append("SELECT * FROM V_T");
            break;
        default:
            fail();
        }
        switch (vOrder) {
        case 1:
            break;
        case 2:
            if (v == 0) {
                fail();
            }
            builder.append(" ORDER BY V ASC");
            break;
        case 3:
            if (v == 0) {
                fail();
            }
            builder.append(" ORDER BY V DESC");
            break;
        default:
            fail();
        }
        builder.append(')');
        switch (qOrder) {
        case 1:
            break;
        case 2:
            builder.append(" ORDER BY A ASC");
            break;
        case 3:
            builder.append(" ORDER BY A DESC");
            break;
        default:
            fail();
        }
        PreparedStatement prep = conn.prepareStatement(builder.toString());
        ResultSet rs = prep.executeQuery();
        switch (qOrder) {
        case 1: {
            HashSet<Integer> set = new HashSet<>();
            for (int i = 0; i < 3; i++) {
                assertTrue(rs.next());
                set.add(rs.getInt(1));
            }
            assertFalse(rs.next());
            assertTrue(set.contains(1));
            assertTrue(set.contains(2));
            assertTrue(set.contains(4));
            break;
        }
        case 2:
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
            break;
        case 3:
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            break;
        }
        prep.close();
        builder.insert(0, "EXPLAIN ");
        rs = stat.executeQuery(builder.toString());
        rs.next();
        String plan = rs.getString(1);
        boolean expectedQuerySorted = q > 1 && qOrder > 1;
        boolean querySorted = plan.endsWith("/* index sorted */");
        assertEquals(expectedQuerySorted, querySorted);
    }

    private void testConditionAndOrDistributiveLaw() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TABLE_A (" +
                "id int NOT NULL AUTO_INCREMENT, " +
                "name VARCHAR(30) NOT NULL," +
                "occupation VARCHAR(20)," +
                "age int," +
                "salary int," +
                "PRIMARY KEY(id))");
        stat.execute("INSERT INTO TABLE_A (name,occupation,age,salary) VALUES" +
                "('mark', 'doctor',25,5000)," +
                "('kevin', 'artist',20,4000)," +
                "('isuru', 'engineer',25,5000)," +
                "('josaph', 'businessman',30,7000)," +
                "('sajeewa', 'analyst',24,5000)," +
                "('randil', 'engineer',25,5000)," +
                "('ashan', 'developer',24,5000)");
        ResultSet rs = stat.executeQuery("SELECT * FROM TABLE_A WHERE (salary = 5000 AND name = 'isuru') OR" +
                "(age = 25 AND name = 'isuru') ");
        rs.next();
        assertTrue("engineer".equals(rs.getString("occupation")));
        conn.close();
    }

    private void testConditionsStackOverflow() throws SQLException {
        deleteDb("optimizations");
        Connection conn = getConnection("optimizations");
        Statement stat = conn.createStatement();
        StringBuilder b = new StringBuilder("SELECT 1");
        for (int i=0; i<10000; i++) {
            b.append(" AND 1");
        }
        ResultSet rs = stat.executeQuery(b.toString());
        rs.next();
        assertTrue(rs.getBoolean(1));
        conn.close();
    }
}
