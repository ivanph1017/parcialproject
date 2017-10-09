package ulima.edu.pe.facilito

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

import ulima.edu.pe.facilito._

object GetDataset {

  var columnsNumber : Int = 0

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
  def getAllDataset() : RDD[List[Double]] =
    getRows().map( stringArray => {
      val valueList = ListBuffer[Double]()
      stringArray.foreach(str => valueList.append( str.toDouble ) )
      valueList.toList
    } )

  def getColumnsNumber() : Int = {
    if( columnsNumber == 0 ) {
      this.columnsNumber = getRows().first().length
    }
    this.columnsNumber
  }

}
