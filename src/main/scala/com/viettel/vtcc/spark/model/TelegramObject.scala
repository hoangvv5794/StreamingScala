package com.viettel.vtcc.spark.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
case class TelegramObject(
                           @JsonProperty("id") id: String,
                           @JsonProperty("date") date: String,
                           @JsonProperty("text") text: String,
                           @JsonProperty("from") from: String
                         )
