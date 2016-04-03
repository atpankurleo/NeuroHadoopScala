package edu.pa

import java.io.IOException

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroMultipleOutputs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.HashMap


/**
  * Created by Ankur on 3/22/2016.
  */
object Convolution {
  val SIGNAL_BUFFER_SIZE: Int = 16777216
  val KERNEL_START_FREQ: Short = 5
  val KERNEL_END_FREQ: Short = 200
  val KERNEL_WINDOW_SIZE: Int = 2001
  private var kernelMap: HashMap[Integer, String] = null
  var kernelStack: Array[Array[Short]] = Array.ofDim(KERNEL_END_FREQ + 1, KERNEL_WINDOW_SIZE)
  private var n: Int = 0
  private var signal: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var kernel: Array[Float] = new Array[Float](SIGNAL_BUFFER_SIZE)
  private var timestamp: Array[Long] = new Array[Long](SIGNAL_BUFFER_SIZE)
  private var rec: RatInputFormat = null
  private var lastTimestamp: Long = 0
  private var tempTime: Long = 0L
  private var fn: String = null
  private var multipleOutputs: AvroMultipleOutputs = null
  private var rat: Rat = new Rat
  private var outkey: AvroKey[Rat] = new AvroKey[Rat](rat)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("Neurohadoop scala")
    val sc: SparkContext = new SparkContext(conf)
    var lookup = sc.textFile("morlet-2000.dat")
    var broadVar = sc.broadcast(lookup.collect())
    println(broadVar.value)
    var lines = sc.textFile("R192-2009-11-19-CSC10a.csv")
    var partOneLines = sc.parallelize(lines.collect(), 1)
    System.out.println(partOneLines.take(5))
    var parseLines = partOneLines.map(x => new RatInputFormat().parse(x.toString()))
    /*var parseLines = lines.map(x => x.split(","))*/
    println(parseLines.collect())
    var populateArrays = parseLines.map(x => createTimeAndVoltageArray(x))
    println(populateArrays.take(5))
  }

  def createTimeAndVoltageArray(rec: RatInputFormat) {
    try {
      if (lastTimestamp > rec.getTimestamp) {
        throw new IOException("Timestamp not sorted at: " + lastTimestamp + " and " + rec.getTimestamp)
      }
      lastTimestamp = rec.getTimestamp
      timestamp(n) = lastTimestamp
      signal(n) = rec.getVoltage
      n += 1
    }
    catch {
      case ioe: IOException => {
        System.err.println(ioe.getMessage)
        System.exit(0)
      }
    }
  }

  def cleanup() {
    val fft: FloatFFT_1D = new FloatFFT_1D(SIGNAL_BUFFER_SIZE / 2)
    try {
      for (index <- n until SIGNAL_BUFFER_SIZE / 2) {
        signal(index) = 0
      }
      println("Load Data: " + (System.currentTimeMillis() - tempTime))

      // tempTime = System.currentTimeMillis();
      fft.realForwardFull(signal)

      for (k <- KERNEL_START_FREQ.toInt to KERNEL_END_FREQ ) {
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

        for (i <- (SIGNAL_BUFFER_SIZE / 2 - KERNEL_WINDOW_SIZE + 1) * 2 until (SIGNAL_BUFFER_SIZE / 2 - n) * 2 by -2) {
          rat.setTime(timestamp(t).toInt)
          rat.setFrequency(k.toInt)
          rat.setConvolution(Math.pow(kernel(i), 2).toFloat)

          // context.write(outkey, NullWritable.get());
          multipleOutputs.write("AVRO", outkey, NullWritable.get, fn)
          t += 1
        }
      }
    } catch {
      case ioe: IOException => {
        System.err.println(ioe.getMessage)
        System.exit(0)
      }
    }
  }
}
