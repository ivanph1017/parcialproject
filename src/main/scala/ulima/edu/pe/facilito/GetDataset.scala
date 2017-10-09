package ulima.edu.pe.facilito

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

import ulima.edu.pe.facilito._

object GetDataset {

  //Obtener RDD file
  def getFile() : RDD[String] =
    MySparkContext.getSparkContext().textFile("data/HR_dataset_parcial.csv")

  //Obtener filas del CSV
  def getRows() : RDD[Array[String]] =
    getFile().map( x => x.split(",") )

  //Obtener dataset de 1 columna
  def getDatasetXColumn(column: Integer) : RDD[Float] =
    getRows().map( x => x( column ).toFloat )

  //Obtener dataset de todas las columnas como Array de Float
  def getAllDataset() : RDD[Array[Double]] =
    getRows().map( stringArray => {
      val floatArray = ArrayBuffer[Double]()
      stringArray.foreach(str => floatArray.append( str.toDouble ) )
      floatArray.toArray
    } )

  def getColumnsNumber() : Int = getRows().take(1).length

}
