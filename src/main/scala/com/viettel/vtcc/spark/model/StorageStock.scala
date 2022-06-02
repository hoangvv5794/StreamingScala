package com.viettel.vtcc.spark.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
case class StorageStock(@JsonProperty("count") count: Long,
                        @JsonProperty("date") date: String,
                        @JsonProperty("stock") stock: String)
