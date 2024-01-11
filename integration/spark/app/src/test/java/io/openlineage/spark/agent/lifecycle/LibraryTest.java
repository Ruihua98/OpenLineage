/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;

@Slf4j
@Tag("beforeShadowJarTest")
@ExtendWith(SparkAgentTestExtension.class)
class LibraryTest {
  private final TypeReference<Map<String, Object>> mapTypeReference =
      new TypeReference<Map<String, Object>>() {};

  @Test
  void testRdd(@TempDir Path tmpDir, SparkSession spark) throws IOException {
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("ns_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getApplicationJobName())
        .thenReturn("job_name");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b")));

    URL url = Resources.getResource("test_data/data.txt");
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<String> textFile = sc.textFile(url.getPath());

    textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .saveAsTextFile(tmpDir.toString() + "/output");

    sc.stop();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(2))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    assertEquals(2, events.size());

    // verify first event
    RunEvent first = events.get(0);

    assertThat(first.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue(
            "name", "test_rdd.map_partitions_shuffled_map_partitions_hadoop");

    assertThat(first.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "job_name");

    assertThat(first.getInputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(first.getInputs().get(0).getName()).endsWith("test/test_data");
    assertThat(first.getEventType()).isEqualTo(EventType.START);

    // verify second event
    RunEvent second = events.get(1);

    assertThat(second.getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue(
            "name", "test_rdd.map_partitions_shuffled_map_partitions_hadoop");

    assertThat(second.getRun().getFacets().getParent().getJob())
        .hasFieldOrPropertyWithValue("namespace", "ns_name")
        .hasFieldOrPropertyWithValue("name", "job_name");

    assertThat(second.getOutputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(second.getOutputs().get(0).getName()).endsWith("output");
    assertThat(second.getEventType()).isEqualTo(EventType.COMPLETE);

    verifySerialization(events);
  }

  Map<String, Object> stripSchemaURL(Map<String, Object> map) {
    List<String> toRemove = new ArrayList<>();
    for (String key : map.keySet()) {
      if (key.endsWith("schemaURL")) {
        toRemove.add(key);
      } else {
        Object value = map.get(key);
        if (value instanceof Map) {
          stripSchemaURL((Map<String, Object>) value);
        }
      }
    }
    for (String key : toRemove) {
      map.remove(key);
    }
    return map;
  }

  @Test
  void testRDDName(SparkSession spark) {
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Integer> numbers =
        sc.parallelize(IntStream.range(1, 100).mapToObj(Integer::new).collect(Collectors.toList()));
    numbers.setName("numbers");
    JavaRDD<String> transformed =
        numbers.filter(n -> n > 10 && n < 90).map(i -> i * i).map(String::valueOf);
    String s = RddExecutionContext.nameRDD(transformed.rdd());
    assertThat(s).isEqualTo("map_partitions_numbers");
  }

  private void verifySerialization(List<OpenLineage.RunEvent> events)
      throws JsonProcessingException {
    for (OpenLineage.RunEvent event : events) {
      assertNotNull(
          "Event can serialize",
          OpenLineageClientUtils.newObjectMapper().writeValueAsString(event));
    }
  }
}
