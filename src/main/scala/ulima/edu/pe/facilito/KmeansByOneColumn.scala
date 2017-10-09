package ulima.edu.pe.facilito

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder
import scala.util.Random
import scala.math._

import ulima.edu.pe.facilito._

object KmeansByOneColumn {

  //Procesar algoritmo K-means
  def processKmeans(column: Integer, centroidNumber: Integer) {
    //Dataset de 1 columna como RDD
    var datasetRDD = GetDataset.getDatasetXColumn( column )

    //Dataset de 1 columna como Array
    var datasetArray = datasetRDD.collect()

    /*
    Se calcula el tamano de cada seccion a partir del datasetArraySorted
    dividido por el numero de centroides
    */
    var section = datasetArray.length./( centroidNumber )

    //Ordenar datasetArray
    var datasetArraySorted = datasetRDD
    .sortBy( x => x, ascending = true )
    .collect()

    //Arreglo con los valores de los centroides
    var dataAvgArray = addBreakPointAsCentroid( ArrayBuffer[Float](),
      datasetArraySorted, datasetArray.length.-( section ), section )

    //Se crean nuevos centroides
    var initRDD = createNewCentroids( mapToCentroid( datasetRDD,
      dataAvgArray.toArray ) )

    /*
    Se chequea si los valores de los centroides
    son los mismos que de la iteracion anterior, si son iguales, se mapea
    a cadena de texto y se exporta
    */
    checkAvg( initRDD, dataAvgArray.toArray ).map( myTuple => {
        var cad = StringBuilder.newBuilder
        cad.append( myTuple._1.toString ).append( "," )
        cad.append( myTuple._2.mkString( "," ) )
        cad
      } )
      .saveAsTextFile("data/resultado1/")
  }

  //Se crean los centroides a partir de los break points
  def addBreakPointAsCentroid(arr : ArrayBuffer[Float],
    arrSorted : Array[Float], n : Int, section : Int) : ArrayBuffer[Float] = {
    if( n > 0 ) {
      var previousArray =
        addBreakPointAsCentroid(arr, arrSorted, n - section, section)
      previousArray.append( arrSorted( n.+( section./( 2 ) ) ) )
      previousArray
    } else {
      arr.append( arrSorted( 0.+( section./( 2 ) ) ) )
      arr
    }
  }

  /*
  Se mapea cada elemento ( x ) => ( valor del centroide al cual pertenece,
  valor x )
  */
  def mapToCentroid( rdd : RDD[Float], dataAvgArray : Array[Float] )
  : RDD[Tuple2[Float, Float]] =
    rdd.map( x => {
      var minDistance =
        getMinDistance( dataAvgArray, dataAvgArray.length.-( 1 ), x )
      ( minDistance._1, x)
    })

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
    var matchesCount = rdd.cartesian(MySparkContext.getSparkContext()
    .parallelize( dataAvgArray ) )
    .filter( pair => pair._1._1 == pair._2 ).collect()

    /*
    Si no coincide la cantidad de pares de RDD filtrados con la cantidad de
    valores del Array de centroides, se sigue iterando
    */
    if( matchesCount.length != dataAvgArray.length ) {
      var newRDD = createNewCentroids( reMapToCentroid( rdd ) )
      var previousCentroids = rdd
        .map( x => x._1 ).collect()
      checkAvg( newRDD, previousCentroids )
    } else {
      rdd
    }
  }

  /*
  Se mapea cada elemento ( x ) => ( valor del centroide al cual pertenece,
  valor x )
  */
  def reMapToCentroid(rdd : RDD[Tuple2[Float, List[Float]]])
  : RDD[Tuple2[Float, Float]] = {
    // Se obtiene los centroides previos como un Array
    var previousCentroids = rdd.map( x => x._1 ).collect()
    /*
    Se hace flatMapping de cada elemento ( myTuple ) =>
    ( valor del centroide al que pertenece,
    valor de cada elemento de la lista )
    */
    rdd.flatMap( myTuple =>
      for (value <- myTuple._2) yield ( myTuple._1, value ) )
    /*
    Se mapea cada elemento ( x ) => ( valor del nuevo centroide al
    cual pertenece, valor x )
    */
    .map( x => {
      var minDistance = getMinDistance( previousCentroids,
        previousCentroids.length.-( 1 ), x._2 )
      ( minDistance._1, x._2 )
    })
  }

  //Se agrupa
  def createNewCentroids( rdd : RDD[Tuple2[Float, Float]] ) :
  RDD[Tuple2[Float, List[Float]]] =
    rdd
    //Se agrupa por los valor del centroide
    .groupByKey
    //Se mapea los valores pertenientes al centroide como lista
    .mapValues( _.toList )
    /*
    Se crean nuevos centroides:
    (centroide, valuesList) =>
    (Nuevo promedio de la lista del centroide, lista)
    */
    .map( kv => ( kv._2.sum./( kv._2.length ),  kv._2 ) )

  //Se obtiene el centroide mas cercano y su distancia de forma recursiva
  def getMinDistance(arr : Array[Float], n : Integer, x : Float) :
  Tuple2[Float, Float] = {
    if( n > 0 ) {
      /*
      valor de la tuple2[Float, Float] de la
      distancia mínima de la posicion anterior del Array
      */
      var previousDistance = getMinDistance( arr, n - 1 , x)
      /*
      valor absoluto de la distancia al centroide
      del Array en la posicion n
      */
      var currentDistance = abs( x.-( arr( n ) ) )
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
        ( arr( 0 ), abs( x.-( arr( 0 ) ) ) )
    }
  }
}
