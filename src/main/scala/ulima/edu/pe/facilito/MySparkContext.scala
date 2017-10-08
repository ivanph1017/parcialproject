package ulima.edu.pe.facilito

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MySparkContext {

  var sc : SparkContext = null

  def getSparkContext() : SparkContext = {
    if(this.sc == null) {
      var conf = new SparkConf().setAppName("parcialproject").setMaster("local")
      this.sc = new SparkContext(conf)
    }
    return this.sc
  }

}
