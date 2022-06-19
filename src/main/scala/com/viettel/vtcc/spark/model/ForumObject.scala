package com.viettel.vtcc.spark.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
case class ForumObject( @JsonProperty("id") id: String,
                        @JsonProperty("published_time") published_time: String,
                        @JsonProperty("content") content: String,
                        @JsonProperty("title") title: String)
