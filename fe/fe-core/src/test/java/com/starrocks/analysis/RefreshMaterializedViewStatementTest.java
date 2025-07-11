// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class RefreshMaterializedViewStatementTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static PseudoCluster cluster;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");

        starRocksAssert.withTable("create table table_name_tmp_1 ( c1 bigint NOT NULL, c2 string not null, c3 int not null ) " +
                " DISTRIBUTED BY HASH(c1) BUCKETS 1 " +
                " PROPERTIES(\"replication_num\" = \"1\");");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "REFRESH MATERIALIZED VIEW no_exists;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error at line 1, column 26. Detail message: " +
                    "Can not find materialized view:no_exists.", e.getMessage());
        }
    }

    @Test
    public void testRefreshNotMaterializedView() {
        String sql = "REFRESH MATERIALIZED VIEW table_name_tmp_1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error at line 1, column 26. Detail message: " +
                    "Can not refresh non materialized view:table_name_tmp_1.", e.getMessage());
        }
    }

    @Test
    public void testRefreshMaterializedViewWithPriority() throws Exception {
        {
            String sql = "refresh materialized view mv1 with sync mode";
            RefreshMaterializedViewStatement stmt =
                    (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertTrue(stmt.isSync());
            Assertions.assertNull(stmt.getPriority());
        }
        {
            String sql = "refresh materialized view mv1 with priority 70";
            RefreshMaterializedViewStatement stmt =
                    (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertEquals(70, stmt.getPriority().intValue());
        }
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView("create materialized view mv1 distributed by hash(`c1`) " +
                " refresh manual" +
                " as select c1, sum(c3) as total from table_name_tmp_1 group by c1");
        cluster.runSql("test", "insert into table_name_tmp_1 values(1, \"str1\", 100)");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "table_name_tmp_1");
        Assertions.assertNotNull(table);
        Table t2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv1");
        Assertions.assertNotNull(t2);
        MaterializedView mv1 = (MaterializedView) t2;
        cluster.runSql("test", "refresh materialized view mv1 with sync mode");

        MaterializedView.MvRefreshScheme refreshScheme = mv1.getRefreshScheme();
        Assertions.assertNotNull(refreshScheme);
        System.out.println("visibleVersionMap:" + refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap());
        Assertions.assertTrue(refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().containsKey(table.getId()));
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap =
                refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().get(table.getId());
        if (partitionInfoMap.containsKey("table_name_tmp_1")) {
            MaterializedView.BasePartitionInfo partitionInfo = partitionInfoMap.get("table_name_tmp_1");
            Assertions.assertEquals(table.getPartition("table_name_tmp_1").getDefaultPhysicalPartition()
                    .getVisibleVersion(), partitionInfo.getVersion());
        }
    }
}
