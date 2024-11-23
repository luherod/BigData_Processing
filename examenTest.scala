package job.examen

import job.examen.examen.{ejercicio1, ejercicio2, ejercicio3, ejercicio4, ejercicio5}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import utils.TestInit

class examenTest extends TestInit{

  "ejercicio1" should "Mostrar el esquema del DF, nombres de estudiantes por calificación descendente y >8" in{

    import spark.implicits._
    val dfEstudiantes: DataFrame = spark.sparkContext.parallelize(
      Seq(
        ("Ana",     25, 8.5),
        ("Luis",    30, 9.0),
        ("Carlos",  28, 7.5),
        ("Marta",   22, 6.8),
        ("Javier",  35, 8.0),
        ("Laura",   29, 9.2),
        ("Sofía",   27, 7.9),
        ("Pedro",   32, 6.5),
        ("Lucía",   24, 8.3),
        ("Roberto", 31, 7.2)
      )
    ).toDF("nombre","edad","calificación")

    ejercicio1(dfEstudiantes)(spark).show()

    ejercicio1(dfEstudiantes)(spark)
      .head()
      .getAs("nombre")
      .toString shouldBe "Laura"

    ejercicio1(dfEstudiantes)(spark).count() shouldBe 4

  }


  "ejercicio2" should "Determinar si los números de un dataframa son pares o impares" in{

    import spark.implicits._
    val dfNumeros: DataFrame = spark.sparkContext.parallelize(
      Seq(-45, 72, 89, -13, 0, 56, -92, 38, 17, -76)
    ).toDF("Número")

    ejercicio2(dfNumeros)(spark).show()

    ejercicio2(dfNumeros)(spark)
      .filter(col("Número")===17)
      .head()
      .getAs("Clasificación")
      .toString shouldBe "impar"

    ejercicio2(dfNumeros)(spark)
      .filter(col("Número")===72)
      .head()
      .getAs("Clasificación")
      .toString shouldBe "par"

  }


  "ejercicio3" should "Devolver una DF con las notas promedio de cada alumno" in{

    import spark.implicits._
    val dfEstudiantes: DataFrame = spark.sparkContext.parallelize(
      Seq(
        (1,  "Ana"),
        (2,  "Luis"),
        (3,  "Carlos"),
        (4,  "Marta"),
        (5,  "Javier")
      )
    ).toDF("id", "nombre")

    val dfCalificaciones: DataFrame = spark.sparkContext.parallelize(
      Seq(
        (3,  "Matemáticas", 6.8),
        (3,  "Literatura",  7.3),
        (3,  "Física",      8.0),
        (1,  "Matemáticas", 9.2),
        (1,  "Literatura",  8.7),
        (1,  "Física",      7.8),
        (5,  "Matemáticas", 8.0),
        (5,  "Literatura",  6.7),
        (5,  "Física",      7.6),
        (4,  "Matemáticas", 9.5),
        (4,  "Literatura",  7.4),
        (4,  "Física",      8.1),
        (2,  "Matemáticas", 7.3),
        (2,  "Literatura",  9.0),
        (2,  "Física",      8.5)
      )
    ).toDF("id", "asignatura","calificación")

    ejercicio3(dfEstudiantes,dfCalificaciones).show()

    ejercicio3(dfEstudiantes,dfCalificaciones)
      .filter(col("id")===2)
      .head()
      .getAs("calificación promedio")
      .toString
      .toDouble shouldBe 8.27

  }


  "ejercicio4" should "devolver cuantas veces se repite cada palabra en una lista" in{

    val listaPalabras: List[String] = List("manzana", "uva", "cereza", "manzana", "naranja", "melocotón", "uva", "uva")

    import spark.implicits._
    ejercicio4(listaPalabras)(spark).toDF().show()

    ejercicio4(listaPalabras)(spark)
      .filter(_._1=="manzana")
      .first() shouldBe ("manzana",2)
  }


  "ejercicio5" should "devolver el ingreso total por producto" in{

    val ventas: DataFrame = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("src/test/resources/ventas.csv")

    ejercicio5(ventas)(spark).show()
    ejercicio5(ventas)(spark)
      .filter(col("id_producto")===102)
      .head()
      .getAs("ingreso total")
      .toString
      .toDouble shouldBe 405.0
  }

}
