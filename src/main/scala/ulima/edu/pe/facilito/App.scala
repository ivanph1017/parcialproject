package ulima.edu.pe.facilito

import ulima.edu.pe.facilito._

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    //Se procesa la columna 1 con 3 centroides
    KmeansByOneColumn.processKmeans(1,3)
    KmeansByAllColumns.processKmeans(3)
  }

}
