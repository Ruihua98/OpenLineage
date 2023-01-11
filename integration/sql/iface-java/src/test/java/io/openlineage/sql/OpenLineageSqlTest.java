/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class OpenLineageSqlTest {
  @Test
  void basicParse() {
    SqlMeta output = OpenLineageSql.parse(Arrays.asList("SELECT * FROM test")).get();
    assertEquals(
        output,
        new SqlMeta(
            Arrays.asList(new DbTableMeta(null, null, "test")),
            new ArrayList<DbTableMeta>(),
            Collections.emptyList()));
  }

  @Test
  void parseWithDialect() {
    SqlMeta output =
        OpenLineageSql.parse(
                Arrays.asList(
                    "SELECT * FROM `random-project`.`dbt_test1`.`source_table` WHERE id = 1"),
                "bigquery")
            .get();
    assertEquals(
        output,
        new SqlMeta(
            Arrays.asList(new DbTableMeta("random-project", "dbt_test1", "source_table")),
            new ArrayList<DbTableMeta>(),
            Collections.emptyList()));
  }

  @Test
  void parseError() {
    boolean exceptionCaught = false;
    try {
      SqlMeta output = OpenLineageSql.parse(Arrays.asList("Definitely not an SQL statement")).get();
    } catch (Exception e) {
      exceptionCaught = true;
      assertTrue(e instanceof Exception);
    }
    assertTrue(exceptionCaught);
  }

  @Test
  void simpleColumnLevelLineage() {
    SqlMeta output =
        OpenLineageSql.parse(Arrays.asList("SELECT t1.a, b, c as x, d as y FROM table1 t1")).get();
    assertEquals(
        Arrays.asList(
            columnLineage("a", "table1", "a"),
            columnLineage("b", "table1", "b"),
            columnLineage("x", "table1", "c"),
            columnLineage("y", "table1", "d")),
        output.columnLineage());
  }

  @Test
  void columnLevelLineageJoinAndRename() {
    SqlMeta output =
        OpenLineageSql.parse(
                Arrays.asList(
                    "SELECT t1.a as x, t2.b as y\n"
                        + "FROM table1 t1\n"
                        + "INNER JOIN table2 t2\n"
                        + "ON t1.a = t2.a"))
            .get();
    assertEquals(
        Arrays.asList(columnLineage("x", "table1", "a"), columnLineage("y", "table2", "b")),
        output.columnLineage());
  }

  @Test
  void columnLevelLineageRename() {
    SqlMeta output =
        OpenLineageSql.parse(
                Arrays.asList(
                    "SELECT t1.a, t2.c as d\n"
                        + "FROM table1 t1\n"
                        + "INNER JOIN (SELECT t2.a, t2.b as c FROM table2 t2) t2"))
            .get();
    assertEquals(
        Arrays.asList(columnLineage("a", "table1", "a"), columnLineage("d", "table2", "b")),
        output.columnLineage());
  }

  @Test
  void columnLevelLineageMultipleDependencies() {
    SqlMeta output =
        OpenLineageSql.parse(
                Arrays.asList("SELECT CASE WHEN a > b THEN c ELSE a END as d FROM table1\n"))
            .get();
    assertEquals(
        Arrays.asList(
            columnLineage(
                "d",
                Arrays.asList(
                    Pair.of("table1", "a"), Pair.of("table1", "b"), Pair.of("table1", "c")))),
        output.columnLineage());
  }

  @Test
  void columnLevelLineageSimpleOperator() {
    SqlMeta output =
        OpenLineageSql.parse(Collections.singletonList("SELECT t1.a + b as c FROM table1 t1"))
            .get();
    assertEquals(
        Arrays.asList(
            columnLineage("c", Arrays.asList(Pair.of("table1", "a"), Pair.of("table1", "b")))),
        output.columnLineage());
  }

  @Test
  void columnLevelLineageCount() {
    SqlMeta output =
        OpenLineageSql.parse(Collections.singletonList("SELECT COUNT(t1.a) as b FROM table1 t1"))
            .get();
    assertEquals(Arrays.asList(columnLineage("b", "table1", "a")), output.columnLineage());
  }

  @Test
  void columnLevelLineageWindow() {
    SqlMeta output =
        OpenLineageSql.parse(
                Collections.singletonList(
                    "SELECT RANK() OVER (PARTITION BY i.a ORDER BY i.b DESC) AS c FROM table1 i"))
            .get();
    assertEquals(
        Arrays.asList(
            columnLineage("c", Arrays.asList(Pair.of("table1", "a"), Pair.of("table1", "b")))),
        output.columnLineage());
  }

  @Test
  void columnLineageWildcard() {
    SqlMeta output =
        OpenLineageSql.parse(Collections.singletonList("INSERT INTO table_1 SELECT * FROM table_2"))
            .get();
    assertEquals(0, output.columnLineage().size());
  }

  ColumnLineage columnLineage(String columnName, String sourceTable, String sourceColumn) {
    return columnLineage(columnName, Collections.singletonList(Pair.of(sourceTable, sourceColumn)));
  }

  ColumnLineage columnLineage(String columnName, List<Pair<String, String>> sourceColumn) {
    return new ColumnLineage(
        new ColumnMeta(null, columnName),
        sourceColumn.stream()
            .map(
                column ->
                    new ColumnMeta(
                        new DbTableMeta(null, null, column.getLeft()), column.getRight()))
            .collect(Collectors.toList()));
  }
}
