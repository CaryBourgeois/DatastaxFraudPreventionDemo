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

// This implementation uses the Kafka Direct API supported in Spark 1.4+
object TransactionAggregator extends App {
  // have to declare this as @transient lazy as we are using it in the
  //
  @transient lazy val r = scala.util.Random

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

  val ssc = new StreamingContext(sc, Milliseconds(5000))
  ssc.checkpoint(appName)

  val kafkaTopics = Set(systemConfig.getString("TransactionConsumer.kafkaAggTopic"))
  val kafkaParams = Map[String, String]("metadata.broker.list" -> systemConfig.getString("TransactionConsumer.kafkaHost"))

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

  kafkaStream
    .foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        val msg = message.map {
          case (k, v) => v
        }

//        sqlContext.sql("CREATE TEMPORARY TABLE txn_count_sec USING org.apache.spark.sql.cassandra OPTIONS(table \"txn_count_sec\", keyspace, \"rtfap\", pushdown \"true\")")
//
//        val df = sqlContext.sql()

        val df = sqlContext
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "rtfap", "table" -> "txn_count_sec"))
          .load()
          .agg(sum("total_txn"), sum("approved_txn"), sum("declined_txn"))

        df.show()

//        val dfAgg = df.agg(sum("total_txn"), sum("approved_txn"), sum("declined_txn"))

        val nowInMillis = System.currentTimeMillis()
        val recMin = new Timestamp((nowInMillis / 60000) * 60000)
        val recTS = new Timestamp(nowInMillis)
        val totalTxn = df.select("total_txn").first()
        val declinedTxn = df.select("total_txn").first()
        val approvedTxn = df.select("total_txn").first()

        val dfAgg = sc.makeRDD(Seq((recMin, recTS, totalTxn, approvedTxn, declinedTxn)))
          .toDF("epoch_min", "ts", "total_txn", "approved_txn", "declined_txn")

        dfAgg
          .write
          .format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "rtfap", "table" -> "txn_count_min"))
          .save()

        df.show()

      }
    }

  ssc.start()
  ssc.awaitTermination()

}
