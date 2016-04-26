package edu.pa

/**
  * Created by Ankur on 4/18/2016.
  */
import com.esotericsoftware.kryo.Kryo

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}

class RatKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[RatRecord])
  }
}

object RatKryoRegistrator {
  def register(conf: SparkConf) {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[RatKryoRegistrator].getName)
  }
}
