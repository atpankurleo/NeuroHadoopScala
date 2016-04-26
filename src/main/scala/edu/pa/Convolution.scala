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
import org.apache.spark.{TaskContext, Partition, SparkContext, SparkConf}

import scala.collection.mutable
import org.apache.spark.sql.{SaveMode, SQLContext}


/**
  * Created by Ankur on 3/22/2016.
  */
case class RatRecord(time: Int, frequency: Int, convolution: Float)

object Convolution {
  val SIGNAL_BUFFER_SIZE: Int = 16777216
  val KERNEL_START_FREQ: Int = 5
  val KERNEL_END_FREQ: Short = 200
  val KERNEL_WINDOW_SIZE: Int = 2001
  private var kernelMap: mutable.HashMap[Integer, String] = null
  var kernelStack: Array[Array[Short]] = Array.ofDim(KERNEL_END_FREQ + 1, KERNEL_WINDOW_SIZE)
  private var nIndex: Int = 0
  private var signal: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var kernel: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var timestamp: Array[Long] = new Array[Long](SIGNAL_BUFFER_SIZE)
  private var lastTimestamp: Long = 0
  private var tempTime: Long = 0L
  private var fn: String = null
  private var rat: Rat = new Rat
  private var ratRecords: Array[Rat] = new Array[Rat](1294750912)

  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Neurohadoop scala")
    val sparkConf = new SparkConf().setAppName("Neurohadoop scala")
    RatKryoRegistrator.register(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //var lookup = sc.textFile("morlet-2000.dat")
    val lookup = sc.textFile("hdfs:///neuro/lookup/morlet-2000.dat")
    val broadVar = sc.broadcast(lookup.collect())
    println(broadVar.value.length)
    this.loadKernel(broadVar.value)
    this.setup()

    //val lines = sc.textFile("R192-2009-11-19-CSC10a.csv")
    val lines = sc.textFile("hdfs:///neuro/input/R192-2009-11-19-CSC10a.csv")
      .map(_.toString.split(",")).filter( _.length == 2)
    var timestamps = lines.map(x => x(0).trim).collect()
    println("timestamp count: " + timestamps.size)
    var signals = lines.map(x => x(1).trim).collect()
    println("Signals count: " + signals.size)

    for(i <- 0 until timestamps.size) {
      signal(i) = signals(i).toFloat
      timestamp(i) = timestamps(i).toLong
      nIndex += 1
    }
    signals = null
    timestamps = null

    this.cleanup(sc, sqlContext)

    val records = sc.parallelize(ratRecords, 196)
    println("Parallelized rats")
    ratRecords = null
    signal = null
    timestamp = null
    kernel = null
    kernelMap = null
    kernelStack = null

    val withValues = records.map(x => RatRecord(x.getTime, x.getFrequency, x.getConvolution)).toDF()
    //withValues.save("rats.parquet", SaveMode.Append)
    //val withValues = records.map(_).toDF()
    println("Creating avro job")
    withValues.saveAsParquetFile("rats.parquet")



   /* var conf = new Job()
    FileOutputFormat.setOutputPath(conf, new Path(fn))
    val schema = Rat.SCHEMA$
    AvroJob.setOutputKeySchema(conf, schema)
    conf.setOutputFormatClass(classOf[AvroKeyOutputFormat[Rat]])
    println("writing to file begins")
    withValues.saveAsNewAPIHadoopDataset(conf.getConfiguration)*/

  }

  def createOutputFile(ratRecords: Array[Rat], sc: SparkContext, sqlContext: SQLContext) {
    val records = sc.parallelize(ratRecords, 1)
    //val withValues = records.map(x => RatRecord(x.getTime, x.getFrequency, x.getConvolution)).toDf()
    //withValues.save("rats.parquet", SaveMode.Append)
    //val withValues = records.map(_).toDF()
    println("Creating avro job")
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
      //println(kernelFreq + "---- "+ broad(i))
      kernelFreq += 1
    }
    println(kernelMap.size)
  }

  def createTimeAndVoltageArray(rec: RatInputFormat) {
    println("---v= " +rec.getVoltage + " T = "+ rec.getTimestamp)
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

    println("Generated output filename")

    return "rat=" + ratnumber + "/dt=" + sessiondate + "/channel=" + channelid + "/" + ratnumber + "-" + sessiondate + "-" + channelid
  }

  def cleanup(sc: SparkContext, sqlContext: SQLContext) {
    var count:Int = 0;
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

        //println("Complex inverse completed")

        //println("Kernel Window size "+ KERNEL_WINDOW_SIZE)
        //println("NIndex value:" + nIndex)
        //var ratList:List[Rat] = List()
        for (i <- (SIGNAL_BUFFER_SIZE / 2 - KERNEL_WINDOW_SIZE + 1) * 2 until (SIGNAL_BUFFER_SIZE / 2 - nIndex) * 2 by -2) {
          rat.setTime(timestamp(t).toInt)
          rat.setFrequency(k.toInt)
          rat.setConvolution(Math.pow(kernel(i), 2).toFloat)

          t += 1
          ratRecords(count) = rat
          count += 1
        }
        //ratRecords.++(sc.parallelize(ratList,1))
        //println("Ratrecords count:" + ratRecords.size)
      }
      println("total rat count: " + count)
    } catch {
      case ioe: IOException => {
        System.err.println(ioe.getMessage)
        //System.exit(0)
      }
    }
  }
}
