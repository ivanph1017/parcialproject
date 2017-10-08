package ulima.edu.pe.facilito
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder
import scala.util.Random
import scala.math._

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    processKmeans(1,3)
  }

  //Obtener RDD file
  def getFile() : RDD[String] =
    MySparkContext.getSparkContext().textFile("data/HR_dataset_parcial.csv")

  //Obtener filas del CSV
  def getRows(): RDD[Array[String]] =
    getFile().map( x => x.split(",") )

  //Obtener dataset de 1 columna
  def getDatasetXColumn(column: Integer): RDD[Float] =
    getRows().map( x => x( column ).toFloat )

  //Procesar algoritmo K-means
  def processKmeans(column: Integer, centroidNumber: Integer) {
    //Arreglo con los valores de los centroides
    val dataAvgArray = ArrayBuffer[Float]()
    //Dataset de 1 columna como RDD
    val datasetRDD = getDatasetXColumn( column )
    //Dataset de 1 columna como Array
    val datasetArray = datasetRDD.collect()
    //Ordenar datasetArray
    val datasetArraySorted = datasetRDD
    .sortBy( x => x, ascending = true )
    .collect()
    /*
    Se calcula el tamano de cada seccion a partir del datasetArraySorted
    dividido por el numero de centroides
    */
    val section = datasetArraySorted.length./( centroidNumber )
    /*
    A los centroides se les asigna el punto de quiebre
    entre seccion y seccion
    */
    for( i <- 0 to datasetArraySorted.length.-( section ) by section )
      dataAvgArray.append( datasetArraySorted( i.+( section./( 2 ) ) ) )

    /*
    Se mapea cada elemento ( x ) => ( Valor del centroide al cual pertenece,
    valor x )
    */
    val initRDD = datasetRDD.map( x => {
      val minDistance =
        getMinDistance( dataAvgArray.toArray, dataAvgArray.length.-( 1 ), x )
      ( minDistance._1, x)
    })
    //Se agrupa por los valor del centroide
    .groupByKey
    //Se mapea los valores pertenientes al centroide como lista
    .mapValues( _.toList )
    /*
    Se crean nuevos centroides:
    (centroide, valuesList) =>
    (Nuevo promedio de la lista del centroide, lista)
    */
    .map( kv => ( kv._2.sum / kv._2.length,  kv._2 ) )

    /*
    Se chequea si los valores de los centroides
    son los mismos que de la iteracion anterior, si son iguales, se mapea
    a cadena de texto y se exporta
    */
    checkAvg( initRDD, dataAvgArray.toArray ).map( myTuple => {
        val cad = StringBuilder.newBuilder
        cad.append( myTuple._1.toString )
        myTuple._2.foreach( value => cad.append( "," ).append( value.toString ) )
        cad
      } )
      .saveAsTextFile("data/resultado1/")
  }
  /*
  Se chequea si los valores de los centroides
  son los mismos que de la iteracion anterior de forma recursiva
  */
  def checkAvg(rdd : RDD[Tuple2[Float, List[Float]]],
    dataAvgArray : Array[Float]) : RDD[Tuple2[Float, List[Float]]] = {
    /*
    Se produce un conjunto cartesiano:
    - RDD1 de tipo Tuple2[Float, List[Float]]
    - RDD2 de tipo Float
    se convierte en RDD de pares ( Tuple2[Float, List[Float]], Float )

    Se filtran si los centroides son los mismos que de la iteracion pasada
    al comparar el valor Float del RDD1 con el valor Float del RDD2
    */
    val matchesCount = rdd.cartesian(MySparkContext.getSparkContext()
    .parallelize( dataAvgArray ) )
    .filter( pair => pair._1._1 == pair._2 ).collect()

    /*
    Si no coincide la cantidad de pares de RDD filtrados con la cantidad de
    valores del Array de centroides, se sigue iterando
    */
    if( matchesCount.length != dataAvgArray.length ) {
      val rddReAssign = assign( rdd )
      val newCentroids = rddReAssign.map( x => x._1 ).collect()
      checkAvg( rddReAssign, newCentroids )
    } else {
      rdd
    }
  }

  //Se asigna
  def assign(rdd : RDD[Tuple2[Float, List[Float]]])
  : RDD[Tuple2[Float, List[Float]]] = {
    // Se obtiene los centroides previos como un Array
    val previousCentroids = rdd.map( x => x._1 ).collect()
    /*
    Se hace flatMapping de cada elemento ( myTuple ) =>
    ( Valor del centroide al que pertenece,
    valor de cada elemento de la lista )
    */
    rdd.flatMap( myTuple =>
      for (value <- myTuple._2) yield ( myTuple._1, value ) )
    /*
    Se mapea cada elemento ( x ) => ( Valor del nuevo centroide al
    cual pertenece, valor x )
    */
    .map( x => {
      val minDistance = getMinDistance( previousCentroids,
        previousCentroids.length.-( 1 ), x._2 )
      ( minDistance._1, x._2 )
    })
    //Se agrupa por los valor del centroide
    .groupByKey
    //Se mapea los valores pertenientes al centroide como lista
    .mapValues( _.toList )
    /*
    Se crean nuevos centroides:
    (centroide, valuesList) =>
    (Nuevo promedio de la lista del centroide, lista)
    */
    .map( kv => ( kv._2.sum / kv._2.length,  kv._2 ) )
  }

  //Se obtiene el centroide mas cercano y su distancia de forma recursiva
  def getMinDistance(arr : Array[Float], n : Integer, x : Float) :
  Tuple2[Float, Float] = {
    if( n > 0 ) {
      /*
      Valor de la tuple2[Float, Float] de la
      distancia mínima de la posicion anterior del Array
      */
      val previousDistance = getMinDistance( arr, n - 1 , x)
      /*
      Valor absoluto de la distancia al centroide
      del Array en la posicion n
      */
      val currentDistance = abs( x - arr( n ) )
      /*
      Si la distancia anterior es menor, se retorna
      ( valor del centroide de la posicion anterior del Array,
      distancia al centroide de la posicion anterior del Array
      en valor absoluto )
      */
      if( previousDistance._2 < currentDistance ) {
        ( previousDistance._1, previousDistance._2 )
      } else {
        /*
        Si la distancia actual es menor, se retorna
        ( valor del centroide de la posicion actual del Array,
        distancia al centroide de la posicion actual del Array
        en valor absoluto )
        */
        ( arr( n ), currentDistance )
      }
    } else {
        /*
        Se retorna (valor del centroide de la posicion 0,
        distancia al centroide de la posicion 0 del Array
        en valor absoluto )
        */
        ( arr( 0 ), abs( x - arr( 0 ) ) )
    }
  }

}

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
