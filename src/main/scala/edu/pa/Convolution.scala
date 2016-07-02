package edu.pa

import java.io.IOException

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroKeyOutputFormat, AvroJob, AvroMultipleOutputs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.{TaskContext, Partition, SparkContext, SparkConf}

import scala.collection.mutable
import org.apache.spark.sql.{SaveMode, SQLContext}

import scala.reflect.internal.util.TableDef.Column


/**
  * Created by Ankur on 3/22/2016.
  */
case class RatRecord(time: Int, frequency: Int, convolution: Float)
case class PhaseBucket(time: Int, phaserange: Int)

object Convolution {
  val SIGNAL_BUFFER_SIZE: Int = 16777216
  val KERNEL_START_FREQ: Int = 5
  val KERNEL_END_FREQ: Short = 200
  val KERNEL_WINDOW_SIZE: Int = 2001
  val numBuckets: Int = 75
  val maxphase: Int = 3141594
  val minPhase: Int = -3141593
  val ratRecordSize: Int = 7000000
  val inputFile: String = "R192-2009-11-19-CSC10a.csv"
  private var kernelMap: mutable.HashMap[Integer, String] = null
  var kernelStack: Array[Array[Short]] = Array.ofDim(KERNEL_END_FREQ + 1, KERNEL_WINDOW_SIZE)
  private var nIndex: Int = 0
  private var signal: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var kernel: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var timestamp: Array[Long] = new Array[Long](SIGNAL_BUFFER_SIZE)
  private var lastTimestamp: Long = 0
  private var tempTime: Long = 0L
  private var fn: String = null
  var ratRDD: RDD[RatRecord] = null
  private var counter:Int = 0
  //private var ratRecords: Array[RatRecord] = new Array[RatRecord](1294750912)
  private var ratRecords: Array[RatRecord] = new Array[RatRecord](ratRecordSize)

  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Neurohadoop scala")
    val sparkConf = new SparkConf().setAppName("Neurohadoop scala")
    RatKryoRegistrator.register(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //var lookup = sc.textFile("morlet-2000.dat")
    var lookup = sc.textFile("hdfs:///neuro/lookup/morlet-2000.dat")
    var broadVar = sc.broadcast(lookup.collect())
    println(broadVar.value.length)
    this.loadKernel(broadVar.value)
    this.setup()

    //var lines = sc.textFile("R192-2009-11-19-CSC10a.csv")
    var lines = sc.textFile("hdfs:///neuro/input/" + inputFile)
      .map(_.toString.split(",")).filter( _.length == 2)
    var timestamps = lines.map(x => x(0).trim).collect()
    //println("timestamp count: " + timestamps.size)
    var signals = lines.map(x => x(1).trim).collect()
    //println("Signals count: " + signals.size)

    for(i <- 0 until timestamps.size) {
      signal(i) = signals(i).toFloat
      timestamp(i) = timestamps(i).toLong
      nIndex += 1
    }
    signals = null
    timestamps = null
    lines = null
    lookup = null
    broadVar = null

    this.cleanup(sc, sqlContext)

    //val records = sc.parallelize(ratRecords, 196)
    //ratRDD.saveAsTextFile("ratrecordstext")
    println("Parallelized rats")
    ratRecords = null
    signal = null
    timestamp = null
    kernel = null
    kernelMap = null
    kernelStack = null
    ratRDD = null

    //val convolutiondata = sqlContext.load("rats.parquet")
    var convolutiondata = sqlContext.parquetFile("rats")
    //println("Generated Rat records" + convolutiondata.count())
    //convolutiondata.show(20)
    //convolutiondata.printSchema()
    /*var tempratsavg = convolutiondata.select("time","frequency","dt","convolution").groupBy("time", "frequency").agg(Map(
      "convolution" -> "avg"
    ))*/
    val dtCol = convolutiondata.col("dt")
    val channelCol = convolutiondata.col("channel")
    var tempratsavg = convolutiondata.select("time","frequency","dt","convolution").groupBy("time", "frequency").agg(Map(
      "convolution" -> "avg"
    ))

    tempratsavg = tempratsavg.withColumnRenamed("time", "time1").withColumnRenamed("frequency", "frequency1")
    println("Join")
    //tempratsavg.printSchema()
    //tempratsavg.show(20)
    //val joinRatsavg = convolutiondata.join(tempratsavg,  convolutiondata("time").as("time1") === tempratsavg("time").as("time2") && convolutiondata("frequency").as("f1") === tempratsavg("frequency").as("f2")).distinct
    var joinRatsavg = convolutiondata.join(tempratsavg,  convolutiondata("time") === tempratsavg("time1") && convolutiondata("frequency") === tempratsavg("frequency1")).select("rat", "frequency", "channel", "dt", "avg(convolution)", "time").distinct
    joinRatsavg = joinRatsavg.withColumnRenamed("avg(convolution)", "convolution")
    println("print joinrats schema")
    //joinRatsavg.printSchema()
    //joinRatsavg.show(20)

    /*var temp1 = tempratsavg.withColumn("dt",dtCol).withColumn("channel", channelCol)
    temp1.printSchema()
    temp1.show(20)*/
    //println("Ratsaverage :" + joinRatsavg.count())
    joinRatsavg.registerTempTable("ratsaverage")
    tempratsavg = null
    convolutiondata = null
    //println(joinRatsavg.count())

    /*val ratsaverage = sqlContext.sql("select time, frequency, AVG(convolution) as convolution from convolutiondata where rat='R192' and dt='2009-11-19' group by time, frequency")
    ratsaverage.printSchema()
    ratsaverage.show(20)
    ratsaverage.registerTempTable("ratsaverage")*/

   /* var ratstats = joinRatsavg.select("time","frequency","dt","channel", "convolution", "rat").groupBy("rat", "dt", "channel", "frequency").agg(Map(
      "convolution" -> "avg",
      "convolution" -> "stddev_pop"
    ))*/

    var ratstats = sqlContext.sql("select rat, dt, channel, frequency, CAST(AVG(convolution) AS FLOAT) as mean, CAST(STDDEV_POP(convolution) AS FLOAT) as sd from ratsaverage group by rat, dt, channel, frequency")
    println("ratsstats schema")
    ratstats.printSchema()
    //ratstats.show(20)
    ratstats.registerTempTable("ratstats")
    //println("Ratstats :" + ratstats.count())

    ratstats = null

    println("generating where condition")
    var passesrecords = sc.textFile("hdfs:///neuro/output/passes/R192-2009-11-19_Passes.csv").map(_.toString.split(","))
    val mintimes = passesrecords.map(x => x(0).trim).collect()
    val maxtimes = passesrecords.map(x => x(1).trim).collect()
    val whereCondition = this.concatWhereCondition(mintimes, maxtimes)
    println(whereCondition)

    //joinRatsavg = joinRatsavg.withColumnRenamed("rat", "rat1").withColumnRenamed("dt", "dt1").withColumnRenamed("channel", "channel1").withColumnRenamed("frequency", "frequency1")
    //var ratssubset = ratstats.join(joinRatsavg,  ratstats("rat") === joinRatsavg("rat") && ratstats("dt") === joinRatsavg("dt") && ratstats("channel") === joinRatsavg("channel") && ratstats("frequency") === joinRatsavg("frequency")).select("rat", "dt", "channel", "time", "frequency" ,"((convolution - mean)/sd)").where(joinRatsavg("time").equalTo(""))

    var ratssubset = sqlContext.sql("select r.rat, r.time, r.dt, r.time, r.channel, r.frequency, CAST(((r.convolution - s.mean)/s.sd) AS FLOAT) as convolution from ratsaverage r join ratstats s on (r.rat = s.rat AND r.dt = s.dt AND r.channel = s.channel AND r.frequency = s.frequency) where " + whereCondition)
    ratssubset.printSchema()
    ratssubset.registerTempTable("ratssubset")
    println("Ratssubset")
    //println("Ratssubset records:" + ratssubset.count())
    //ratssubset.show(20)

    var phasebuckets = sc.textFile("hdfs:///neuro/output/phase/R192-2009-11-19-Phase.csv").map(_.toString.split(",")).map(p => PhaseBucket(p(0).trim.toInt, (Math.floor(((p(1).trim.toInt - minPhase).toDouble/(maxphase - minPhase).toDouble) * numBuckets) + 1).toInt)).toDF()
    println("Phasebuckets")
    //println("Phasebucket count: " +phasebuckets.count())
    phasebuckets.registerTempTable("phasebuckets")
    //phasebuckets.rdd.saveAsTextFile("phasebuckets")

    var results = sqlContext.sql("select r.rat, r.dt, r.channel, r.frequency, p.phaserange, CAST(AVG(r.convolution) AS FLOAT) as convolution from ratssubset r join phasebuckets p on (r.time = p.time) group by r.rat, r.dt, r.channel, r.frequency, p.phaserange")
    results.printSchema()
    //println("Results: " + results.count())
    //results.show(20)


    var finalResult = results.filter(results("rat").equalTo("R192") && results("dt").equalTo("2009-11-19")).select("frequency", "phaserange", "convolution")
    finalResult.rdd.coalesce(1, false).saveAsTextFile("resultspark")
    println("result generated")
    //val withValues = records.map(x => RatRecord(x.getTime, x.getFrequency, x.getConvolution)).toDF()
    //val withValues = ratRDD.map(_.split(",")).map(x => RatRecord(x(0).toInt, x(1).toInt, x(2).toFloat)).toDF()
    //val withValues = records.toDF()
    //withValues.save("rats.parquet", SaveMode.Append)
    //val withValues = records.map(_).toDF()
    //println("Creating avro job")
    //withValues.saveAsParquetFile("rats.parquet")

   /* var conf = new Job()
    FileOutputFormat.setOutputPath(conf, new Path(fn))
    val schema = Rat.SCHEMA$
    AvroJob.setOutputKeySchema(conf, schema)
    conf.setOutputFormatClass(classOf[AvroKeyOutputFormat[Rat]])
    println("writing to file begins")
    withValues.saveAsNewAPIHadoopDataset(conf.getConfiguration)*/

  }

  def concatWhereCondition(minTimes: Array[String], maxTimes: Array[String]): String = {
    var whereCondition = ""
    for(i <- 0 until minTimes.size) {
      whereCondition = whereCondition + "((r.time >= " + minTimes(i) + ") and (r.time <= " + maxTimes(i) + "))"
      if(i < minTimes.size - 1) {
        whereCondition = whereCondition + " OR "
      }
    }
    whereCondition
  }

  def createOutputFile(sc: SparkContext, sqlContext: SQLContext) {
    import sqlContext.implicits._
    ratRDD = sc.parallelize(ratRecords, 1)
    var withValues = ratRDD.toDF()
    //val withValues = ratRDD.map(_.split(",")).map(x => RatRecord(x(0).toInt, x(1).toInt, x(2).toFloat)).toDF()
    //val withValues = records.map(x => RatRecord(x.getTime, x.getFrequency, x.getConvolution)).toDf()
    //println("Started creating out file")
    withValues.save("rats/" + this.generateFileName(inputFile), SaveMode.Append)
  }

  def ConvertStringArrayToShortArray(stringArray: Array[String]): Array[Short] = {
    val shortArray: Array[Short] = new Array[Short](stringArray.length)
    for (i <- 0 until stringArray.length) {
      shortArray(i) = (stringArray(i)).toShort
    }
    return shortArray
  }

  def setup() {
    fn = this.generateFileName("R192-2009-11-19-CSC10a.csv")
    for (index <- KERNEL_START_FREQ to KERNEL_END_FREQ) {
      val str:String = kernelMap.getOrElse(index, null)
      //println("index :" + index + "value: "+str)
      kernelStack(index) = ConvertStringArrayToShortArray(str.split(","))
    }
  }

  def loadKernel(broadval : Array[String]) {
    val broad: Array[String] = broadval
    var kernelFreq: Integer = KERNEL_START_FREQ
    this.kernelMap = new mutable.HashMap[Integer, String]
    for(i <- 0 until broadval.length ) {
      this.kernelMap += (kernelFreq -> broad(i))
      kernelFreq += 1
    }
    println(kernelMap.size)
  }

  def createTimeAndVoltageArray(rec: RatInputFormat) {
    try {
      if (lastTimestamp > rec.getTimestamp) {
        throw new IOException("Timestamp not sorted at: " + lastTimestamp + " and " + rec.getTimestamp)
      }
      lastTimestamp = rec.getTimestamp
      timestamp(nIndex) = lastTimestamp
      signal(nIndex) = rec.getVoltage
      nIndex += 1
    }
    catch {
      case ioe: IOException => {
        System.err.println(ioe.getMessage)
        System.exit(0)
      }
    }
  }

  def generateFileName(fname: String):String = {
    var ratnumber: String = null
    var sessiondate: String = null
    var channelid: String = null

    var indexBegin: Int = 0
    var indexEnd: Int = fname.indexOf('-')

    ratnumber = fname.substring(indexBegin, indexEnd)
    indexBegin = indexEnd + 1
    indexEnd = fname.indexOf('-', indexBegin)
    sessiondate = fname.substring(indexBegin, indexEnd)
    indexBegin = indexEnd + 1
    indexEnd = fname.indexOf('-', indexBegin)
    sessiondate = sessiondate + '-' + fname.substring(indexBegin, indexEnd)
    indexBegin = indexEnd + 1
    indexEnd = fname.indexOf('-', indexBegin)
    sessiondate = sessiondate + '-' + fname.substring(indexBegin, indexEnd)
    indexBegin = indexEnd + 4
    indexEnd = fname.indexOf('.', indexBegin)
    channelid = fname.substring(indexBegin, indexEnd)

    return "rat=" + ratnumber + "/dt=" + sessiondate + "/channel=" + channelid
  }

  def cleanup(sc: SparkContext, sqlContext: SQLContext) {
    import sqlContext.implicits._
    var count:Int = 0
    println("Starting Convolution")
    val fft: FloatFFT_1D = new FloatFFT_1D(SIGNAL_BUFFER_SIZE / 2)
    try {
      //println("nIndex before convolution : " + nIndex)
      for (index <- nIndex until SIGNAL_BUFFER_SIZE / 2) {
        signal(index) = 0
      }
      println("Load Data: " + (System.currentTimeMillis() - tempTime))

      // tempTime = System.currentTimeMillis();
      fft.realForwardFull(signal)

      for (k <- KERNEL_START_FREQ to KERNEL_END_FREQ ) {
        for (j <- 0 until KERNEL_WINDOW_SIZE) {
          kernel(j) = kernelStack(k)(j)
        }
        for (j <- KERNEL_WINDOW_SIZE until SIGNAL_BUFFER_SIZE / 2) {
          kernel(j) = 0
        }

        fft.realForwardFull(kernel)

        var temp: Float = 0
        for (i <- 0 until SIGNAL_BUFFER_SIZE by 2) {
          temp = kernel(i)
          kernel(i) = kernel(i) * signal(i) - kernel(i + 1) * signal(i + 1)
          kernel(i + 1) = -(temp * signal(i + 1) + kernel(i + 1) * signal(i))
        }
        fft.complexInverse(kernel, true)

        var t: Int = KERNEL_WINDOW_SIZE - 1

        for (i <- (SIGNAL_BUFFER_SIZE / 2 - KERNEL_WINDOW_SIZE + 1) * 2 until (SIGNAL_BUFFER_SIZE / 2 - nIndex) * 2 by -2) {

          ratRecords(count) = RatRecord(timestamp(t).toInt, k.toInt, Math.pow(kernel(i), 2).toFloat)
          //ratRecords(count) = timestamp(t).toInt + ","+ k.toInt +","+ Math.pow(kernel(i), 2).toFloat
          //println(rat.getTime + "--" + rat.getFrequency + "----"+ rat.getConvolution)
          t += 1

          //ratRecords(count) = rat
          count += 1
          if(count == ratRecordSize) {
            this.createOutputFile(sc, sqlContext)
            //ratRDD = (ratRDD ++ sc.parallelize(ratRecords.slice(0, count), 196)).coalesce(196)
            //throw new IOException("Self created exception occured")
            println("Final count: "  + count)
            count = 0
          }
        }
      }
      if( count != 0 ) {
        ratRecords = ratRecords.slice(0, count)
        this.createOutputFile(sc, sqlContext)
      }
    } catch {
      case ioe: IOException => {
        System.err.println(ioe.getMessage)
        //System.exit(0)
      }
    }
  }
}
