import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor
import cats.effect.{IO, Resource}
import scala.io.StdIn.readLine

object PeliculasCRUD {

  val xa: Resource[IO, HikariTransactor[IO]] = for {
    ce <- ExecutionContexts.fixedThreadPool(10)
    xa <- HikariTransactor.newHikariTransactor[IO](
      "com.mysql.cj.jdbc.Driver",
      "C:/Users/Personal/Desktop/ProyectoPracticum/pi_movies_complete.csv",
      "cody",
      "barderos2005.",
      ce
    )
  } yield xa

  def insertarPelicula(id: Long, title: String, budget: Long, revenue: Long, vote_average: Int, vote_count: Long): ConnectionIO[Int] = {
    sql"""INSERT INTO PELICULA (id, title, budget, revenue, vote_average, vote_count)
         VALUES ($id, $title, $budget, $revenue, $vote_average, $vote_count)"""
      .update.run
  }

  def actualizarPelicula(id: Long, title: String, budget: Long, revenue: Long, vote_average: Int, vote_count: Long): ConnectionIO[Int] = {
    sql"""UPDATE PELICULA SET title = $title, budget = $budget, revenue = $revenue,
         vote_average = $vote_average, vote_count = $vote_count WHERE id = $id"""
      .update.run
  }

  def eliminarPelicula(id: Long): ConnectionIO[Int] = {
    sql"DELETE FROM PELICULA WHERE id = $id".update.run
  }

  def obtenerPeliculas: ConnectionIO[List[(Long, String, Long, Long, Int, Long)]] = {
    sql"SELECT id, title, budget, revenue, vote_average, vote_count FROM PELICULA".query[(Long, String, Long, Long, Int, Long)].to[List]
  }

  def main(args: Array[String]): Unit = {
    xa.use { transactor =>
      IO {
        var continuar = true
        while (continuar) {
          println("Selecciona una opción:")
          println("1. Insertar Película")
          println("2. Actualizar Película")
          println("3. Eliminar Película")
          println("4. Mostrar Películas")
          println("5. Salir")

          readLine() match {
            case "1" =>
              println("Ingrese ID de la película:")
              val id = readLine().toLong
              println("Ingrese título:")
              val title = readLine()
              println("Ingrese presupuesto:")
              val budget = readLine().toLong
              println("Ingrese ingresos:")
              val revenue = readLine().toLong
              println("Ingrese promedio de votos:")
              val vote_average = readLine().toInt
              println("Ingrese conteo de votos:")
              val vote_count = readLine().toLong

              insertarPelicula(id, title, budget, revenue, vote_average, vote_count)
                .transact(transactor)
                .unsafeRunSync()

            case "2" =>
              println("Ingrese ID de la película a actualizar:")
              val id = readLine().toLong
              println("Ingrese nuevo título:")
              val title = readLine()
              println("Ingrese nuevo presupuesto:")
              val budget = readLine().toLong
              println("Ingrese nuevos ingresos:")
              val revenue = readLine().toLong
              println("Ingrese nuevo promedio de votos:")
              val vote_average = readLine().toInt
              println("Ingrese nuevo conteo de votos:")
              val vote_count = readLine().toLong

              actualizarPelicula(id, title, budget, revenue, vote_average, vote_count)
                .transact(transactor)
                .unsafeRunSync()

            case "3" =>
              println("Ingrese ID de la película a eliminar:")
              val id = readLine().toLong

              eliminarPelicula(id)
                .transact(transactor)
                .unsafeRunSync()

            case "4" =>
              val peliculas = obtenerPeliculas.transact(transactor).unsafeRunSync()
              peliculas.foreach(println)

            case "5" =>
              continuar = false

            case _ =>
              println("Opción no válida.")
          }
        }
      }
    }.unsafeRunSync()
  }
}