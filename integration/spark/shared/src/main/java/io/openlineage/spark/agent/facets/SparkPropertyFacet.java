/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkPropertyFacet extends OpenLineage.DefaultRunFacet {
  @JsonProperty("properties")
  @SuppressWarnings("PMD")
  private Map<String, Object> properties;
  private static final String ALLOWED_KEY = "spark.openlineage.capturedProperties";

  public Map<String, Object> getProperties() {
    return properties;
  }

  public SparkPropertyFacet(Map<String, Object> environmentDetails) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.properties = environmentDetails;
  }

  public SparkPropertyFacet(){
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    properties=new HashMap<>();
    try{
      SparkSession session = SparkSession.active();
      Set<String> allowerProperties = new HashSet<>();
      SparkContext context = session.sparkContext();
      SparkConf conf = context.getConf();

      if(conf.contains(ALLOWED_KEY)){
        allowerProperties = Arrays.stream(conf.get(ALLOWED_KEY).split(",")).collect(Collectors.toSet());
      }

      for(String key: allowerProperties){
        try {
          String value = session.conf().get(key);
          properties.putIfAbsent(key, value);
        }catch(NoSuchElementException e){
          // We get keys in capturedProperties from static config captured, while we get key-values from run time config session.conf().
          log.info("A key in capturedProperties not exists in Runtime Config", key);
        }

      }
    }catch(IllegalStateException ie){
      properties=new HashMap<>();
    }
  }
}
