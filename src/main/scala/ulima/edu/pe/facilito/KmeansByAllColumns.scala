package ulima.edu.pe.facilito

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder
import scala.util.Random
import scala.math._

import ulima.edu.pe.facilito._

object KmeansByAllColumns {

  //Procesar algoritmo K-means
  def processKmeans(centroidNumber: Integer) {
    //Arreglo con los valores de los centroides
    val dataAvgArray = ArrayBuffer[Array[Double]]()
    //Dataset completo como RDD
    val datasetRDD = GetDataset.getAllDataset()
    //Dataset de 1 columna como Array
    val datasetArray = datasetRDD.collect()
    /*
    Se calcula el tamano de cada seccion a partir del datasetArray
    dividido por el numero de centroides
    */
    val section = datasetArray.length./( centroidNumber )
    for( i <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
      //Arreglo con las dimensiones de un centroide
      val dataAvgSingleArray = ArrayBuffer[Double]()
      //Ordenar datasetArray por columna
      val datasetSingleArraySorted = datasetRDD
      .map( x => x( i ) )
      .sortBy( x => x, ascending = true )
      .collect()
      /*
      A los centroides se les asigna el punto de quiebre
      entre seccion y seccion
      */
      for( i <- 0 to datasetSingleArraySorted.length.-( section ) by section )
        dataAvgSingleArray
        .append( datasetSingleArraySorted( i.+( section./( 2 ) ) ) )
      //Agregar arreglo dimensional al arreglo de centroides
      dataAvgArray.append( dataAvgSingleArray.toArray )
    }
    /*
    Se mapea cada elemento ( x ) => ( Valor del centroide al cual pertenece,
    conjunto de valores x )
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
    .map( kv => {
      val newCentroid = ArrayBuffer[Double]()
      for( i <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
        val sum = 0.0
        kv._2.foreach( x => sum.+( x( i ) ) )
        newCentroid.append( sum./( kv._2.length ) )
      }
      ( newCentroid.toArray,  kv._2 )
    } )

    /*
    Se chequea si los valores de los centroides
    son los mismos que de la iteracion anterior, si son iguales, se mapea
    a cadena de texto y se exporta
    */
    checkAvg( initRDD, dataAvgArray.toArray ).map( myTuple => {
        val cad = StringBuilder.newBuilder
        cad.append( myTuple._1.toString )
        myTuple._2.foreach( valueArray => {
            cad.append( "," )
            cad.append( "\"(" ).append( valueArray( 0 ) )
            for( i <- 1 to valueArray.length.-( 1 ) )
              cad.append( "," ).append( valueArray( i ).toString )
            cad.append( ")\"" )
        } )
        cad
      } )
      .saveAsTextFile("data/resultado2/")
  }
  /*
  Se chequea si los valores de los centroides
  son los mismos que de la iteracion anterior de forma recursiva
  */
  def checkAvg(rdd : RDD[Tuple2[Array[Double], List[Array[Double]]]],
    dataAvgArray : Array[Array[Double]]) :
    RDD[Tuple2[Array[Double], List[Array[Double]]]] = {
    /*
    Se produce un conjunto cartesiano:
    - RDD1 de tipo Tuple2[Array[Double], List[Double]]
    - RDD2 de tipo Array[Double]
    se convierte en RDD de pares ( Tuple2[Array[Double], List[Double]], Array[Double] )

    Se filtran si los centroides son los mismos que de la iteracion pasada
    al comparar el valor Double del RDD1 con el valor Double del RDD2
    */
    val matchesCount = rdd.cartesian(MySparkContext.getSparkContext()
    .parallelize( dataAvgArray ) )
    .filter( pair => pair._1._1.sameElements( pair._2 ) ).collect()

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
  def assign(rdd : RDD[Tuple2[Array[Double], List[Array[Double]]]])
  : RDD[Tuple2[Array[Double], List[Array[Double]]]] = {
    // Se obtiene los centroides previos como un Array[Array[Double]]
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
    cual pertenece, conjunto de valores de x )
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
    .map( kv => {
      val newCentroid = ArrayBuffer[Double]()
      for( i <- 0 to GetDataset.getColumnsNumber().-( 1 ) ) {
        val sum = 0.0
        kv._2.foreach( x => sum.+( x( i ) ) )
        newCentroid.append( sum./( kv._2.length ) )
      }
      ( newCentroid.toArray,  kv._2 )
    } )
  }

  //Se obtiene el centroide mas cercano y su distancia de forma recursiva
  def getMinDistance(arr : Array[Array[Double]], n : Integer,
    x : Array[Double]) : Tuple2[Array[Double], Double] = {
    if( n > 0 ) {
      /*
      Valor de la tuple2[Double, Double] de la
      distancia mínima de la posicion anterior del Array
      */
      val previousDistance = getMinDistance( arr, n - 1 , x)
      /*
      Valor absoluto de la distancia al centroide
      del Array en la posicion n
      */
      val currentDistance = getEuclidanDistance( arr( n ), x )
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
        ( arr( 0 ), getEuclidanDistance( arr( 0 ), x ) )
    }
  }

  def getEuclidanDistance(arr : Array[Double], x : Array[Double]) : Double = {
    val qrtSum = 0
    val arrParallelized = MySparkContext.getSparkContext()
      .parallelize( arr )
    val xParallelized = MySparkContext.getSparkContext()
      .parallelize( x )
    arrParallelized.cartesian( xParallelized )
      .foreach( pair => qrtSum.+( pow( pair._1 - pair._2, 2 ) ) )
    sqrt( qrtSum )
  }
}
