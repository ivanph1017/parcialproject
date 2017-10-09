package ulima.edu.pe.facilito

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder
import scala.collection.immutable.List

import scala.util.Random
import scala.math._

import ulima.edu.pe.facilito._

object KmeansByAllColumns {

  //Procesar algoritmo K-means
  def processKmeans(centroidNumber: Integer) {
    //Lista con los valores de los centroides
    val dataAvgList = ListBuffer[List[Double]]()
    //Dataset completo como RDD
    val datasetRDD = GetDataset.getAllDataset()
    //Dataset de 1 columna como List
    val datasetList = datasetRDD.collect()
    /*
    Se calcula el tamano de cada seccion a partir del datasetList
    dividido por el numero de centroides
    */
    val section = datasetList.length./( centroidNumber )
    for( i <- 0 to datasetList.length.-( 1 ).-( section ) by section ) {
      //Lista con las dimensiones de un centroide
      var dataAvgSingleList = ListBuffer[Double]()
      for( j <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
        //Ordenar datasetList por columna
        var datasetSingleListSorted = datasetRDD
        .map( x => x( j ) )
        .sortBy( x => x, ascending = true )
        .collect()
        /*
        A los centroides se les asigna el punto de quiebre
        entre seccion y seccion
        */
        dataAvgSingleList
        .append( datasetSingleListSorted( i.+( section./( 2 ) ) ) )
      }
      //Agregar Lista dimensional al lista de centroides
      dataAvgList.append( dataAvgSingleList.toList )
    }
    /*
    Se mapea cada elemento ( x ) => ( Valor del centroide al cual pertenece,
    conjunto de valores x )
    */
    val initRDD = datasetRDD.map( x => {
      val minDistance =
        getMinDistance( dataAvgList.toList, dataAvgList.length.-( 1 ), x )
      val strCentroide = minDistance._1.mkString( "," )
      ( strCentroide, x )
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
    .map( kv => {
      val newCentroid = ListBuffer[Double]()
      for( i <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
        var sum = 0.0
        kv._2.foreach( x => sum += x( i ) )
        newCentroid.append( sum./( kv._2.length ) )
      }
      ( newCentroid.toList,  kv._2 )
    } )
    /*
    Se chequea si los valores de los centroides
    son los mismos que de la iteracion anterior, si son iguales, se mapea
    a cadena de texto y se exporta
    */
    checkAvg( initRDD, dataAvgList.toList ).map( myTuple => {
        var cad = StringBuilder.newBuilder
        cad.append( myTuple._1.mkString( "\"(", ",", ")\"") )
        myTuple._2.foreach( valueList => {
            cad.append( "," )
            cad.append( valueList.mkString( "\"(", ",", ")\"") )
        } )
        cad
      } )
     .saveAsTextFile("data/resultado2/")
  }
  /*
  Se chequea si los valores de los centroides
  son los mismos que de la iteracion anterior de forma recursiva
  */
  def checkAvg(rdd : RDD[Tuple2[List[Double], List[List[Double]]]],
    dataAvgList : List[List[Double]]) :
    RDD[Tuple2[List[Double], List[List[Double]]]] = {
    /*
    Se verifica que los centroides anteriores con los nuevos sean iguales
    */
    var isEqual = true
    val keySet = rdd.map( x => x._1 ).collect()
    for( i <- 0 to keySet.length.-( 1 ) )
      isEqual = keySet( i ).sameElements( dataAvgList( i ) ) && isEqual

    if( !isEqual ) {
      var rddAssign = assign( rdd )
      var previousCentroids = rdd.map( x => x._1 ).collect().toList
      checkAvg( rddAssign, previousCentroids )
    } else {
      rdd
    }
  }

  //Se reasigna
  def assign(rdd : RDD[Tuple2[List[Double], List[List[Double]]]])
  : RDD[Tuple2[List[Double], List[List[Double]]]] = {
    // Se obtiene los centroides previos como un List[List[Double]]
    val previousCentroids = rdd.map( x => x._1 ).collect().toList
    /*
    Se hace flatMapping de cada elemento ( myTuple ) =>
    ( Valor del centroide al que pertenece,
    valor de cada elemento de la lista )
    */
    rdd.flatMap( myTuple =>
      for (value <- myTuple._2) yield ( myTuple._1, value ) )
    /*
    Se mapea cada elemento ( x ) => ( Valor del nuevo centroide al
    cual pertenece, conjunto de valores de x )
    */
    .map( x => {
      val minDistance = getMinDistance( previousCentroids,
        previousCentroids.length.-( 1 ), x._2 )
      val strCentroide = minDistance._1.mkString( "," )
      ( strCentroide, x._2 )
    } )
    //Se agrupa por los valor del centroide
    .groupByKey
    //Se mapea los valores pertenientes al centroide como lista
    .mapValues( _.toList )
    /*
    Se crean nuevos centroides:
    (centroide, valuesList) =>
    (Nuevo promedio de la lista del centroide, lista)
    */
    .map( kv => {
      val newCentroid = ListBuffer[Double]()
      for( i <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
        var sum = 0.0
        kv._2.foreach( x => sum += x( i ) )
        newCentroid.append( sum./( kv._2.length ) )
      }
      ( newCentroid.toList,  kv._2 )
    } )
  }

  //Se obtiene el centroide mas cercano y su distancia de forma recursiva
  def getMinDistance(list : List[List[Double]], n : Integer,
    x : List[Double]) : Tuple2[List[Double], Double] = {
    if( n > 0 ) {
      /*
      Valor de la Tuple2[List[Double], Double] de la
      distancia mínima de la posicion anterior del List
      */
      val previousDistance = getMinDistance( list, n - 1 , x)
      /*
      Valor de la distancia al centroide
      del List en la posicion n
      */
      val currentDistance = getEuclidanDistance( list( n ), x )
      /*
      Si la distancia anterior es menor, se retorna
      ( valor del centroide de la posicion anterior del List,
      distancia al centroide de la posicion anterior del List )
      */
      if( previousDistance._2 < currentDistance ) {
        ( previousDistance._1, previousDistance._2 )
      } else {
        /*
        Si la distancia actual es menor, se retorna
        ( valor del centroide de la posicion actual del List,
        distancia al centroide de la posicion actual del List )
        */
        ( list( n ), currentDistance )
      }
    } else {
        /*
        Se retorna (valor del centroide de la posicion 0,
        distancia al centroide de la posicion 0 del List )
        */
        ( list( 0 ), getEuclidanDistance( list( 0 ), x ) )
    }
  }

  //Obtener la distancia euclidiana
  def getEuclidanDistance(list : List[Double], x : List[Double]) : Double = {
    var qrtSum = 0.0
    for( i <- 0 to list.length.-( 1 ) )
     //suma de cuadrados
     qrtSum += pow( list( i ).-( x( i ) ), 2 )
    // raiz cuadrada de suma de cuadrados
    sqrt( qrtSum )
  }
}
