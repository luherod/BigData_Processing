package job.examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object examen {

  /*******************************************************************************************************************
   Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   *******************************************************************************************************************/

  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {

    estudiantes.printSchema()

    estudiantes
      .filter(col("calificación")>8)
      .orderBy(col("calificación").desc)
      .select("nombre")

  }



  /*******************************************************************************************************************
  Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
  ********************************************************************************************************************/

  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {

    def parOImpar(numeros: Integer): String = if(numeros%2==0) "par" else "impar"

    val clasificacionDeNumeros= udf((numeros: Integer) => parOImpar(numeros))

    numeros.withColumn("Clasificación" , clasificacionDeNumeros(col(numeros.columns(0))))

  }



  /*******************************************************************************************************************
  Ejercicio 3: Joins y agregaciones
  Pregunta: Dados dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  ********************************************************************************************************************/

  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {


    estudiantes
      .join(calificaciones, Seq(estudiantes.columns(0), calificaciones.columns(0)) , "inner")
      .groupBy("id","nombre").agg(avg("calificación"))
      .withColumnRenamed("avg(calificación)", "calificación promedio")
      .withColumn("calificación promedio", round(col("calificación promedio"),2))


  }



  /*******************************************************************************************************************
  Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
  ********************************************************************************************************************/

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {

    spark.sparkContext.parallelize(palabras.toSeq)
      .map(palabra => (palabra, palabras.count(_==palabra)))
      .distinct()

  }



  /*******************************************************************************************************************
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  ********************************************************************************************************************/

  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {

    ventas
      .groupBy("id_producto").agg(
        sum(
          col("cantidad")*col("precio_unitario")
        )
      )
      .withColumnRenamed("sum((cantidad * precio_unitario))", "ingreso total")
  }

}
