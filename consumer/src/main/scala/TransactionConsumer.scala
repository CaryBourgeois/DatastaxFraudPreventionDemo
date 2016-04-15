package com.datastax.demo.fraudprevention
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * Created by carybourgeois on 10/30/15.
  */

import java.util.GregorianCalendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import java.util.Calendar

case class Transaction(cc_no:String,
                       cc_provider: String,
                       year: Int,
                       month: Int,
                       day: Int,
                       hour: Int,
                       min: Int,
                       txn_time: Timestamp,
                       txn_id: String,
                       merchant: String,
                       location: String,
                       items: Map[String, Double],
                       amount: Double,
                       status: String)

// This implementation uses the Kafka Direct API supported in Spark 1.4+
object TransactionConsumer extends App {

  val r = scala.util.Random

  /*
   * Get runtime properties from application.conf
   */
  val systemConfig = ConfigFactory.load()

  val appName = systemConfig.getString("TransactionConsumer.sparkAppName")

  val conf = new SparkConf()
    .set("spark.cores.max", "2")
    .set("spark.executor.memory", "512M")
    .setAppName(appName)
  val sc = SparkContext.getOrCreate(conf)

  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val ssc = new StreamingContext(sc, Milliseconds(1000))
  ssc.checkpoint(appName)

  val kafkaTopics = Set(systemConfig.getString("TransactionConsumer.kafkaTopic"))
  val kafkaParams = Map[String, String]("metadata.broker.list" -> systemConfig.getString("TransactionConsumer.kafkaHost"))

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

  kafkaStream
    .foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        val df = message.map {
          case (k, v) => v.split(";")
        }.map(payload => {
          val cc_no = payload(0)
          val cc_provider = payload(1)

          val txn_time = Timestamp.valueOf(payload(2))
          val calendar = new GregorianCalendar()
          calendar.setTime(txn_time)

          val year = calendar.get(Calendar.YEAR)
          val month = calendar.get(Calendar.MONTH)
          val day = calendar.get(Calendar.DAY_OF_MONTH)
          val hour = calendar.get(Calendar.HOUR)
          val min = calendar.get(Calendar.MINUTE)

          val txn_id = payload(3)
          val merchant = payload(4)
          val location = payload(5)
          println(s"Kafka Message Paylod(6): ${payload(6)}")
          val items = payload(6).split(",").map(_.split("->")).map { case Array(k, v) => (k, v.toDouble) }.toMap
          val amount = payload(7).toDouble
          //
          // This need to be updated to include more evaluation rules.
          //
          val status = if (payload(8) != "CHECK") {
            payload(8)
          } else if (0.01 > r.nextGaussian()) {
            "APPOVED"
          } else {
            "DECLINED"
          }

          Transaction(cc_no, cc_provider, year, month, day, hour, min, txn_time, txn_id, merchant, location, items, amount, status)
        }).toDF("cc_no",
                "cc_provider",
                "year",
                "month",
                "day",
                "hour",
                "min",
                "txn_time",
                "txn_id",
                "merchant",
                "location",
                "items",
                "amount",
                "status")

        df
          .write
          .format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "rtfap", "table" -> "transactions"))
          .save()

        df.show(5)
        println(s"${df.count()} rows processed.")
      }
    }

  ssc.start()
  ssc.awaitTermination()
}