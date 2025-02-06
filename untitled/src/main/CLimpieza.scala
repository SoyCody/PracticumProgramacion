import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import java.io.{BufferedWriter, File, FileWriter}

case class NewComplete(adult: String, belongs_to_collection: String, budget: Long, genres: String, homepage: String,
                       id: Long, imd_id: String, original_language: String, original_title: String, overview: String,
                       popularity: Long, poster_path: String, production_companies: String, production_countries: String,
                       release_date: String, revenue: Long, runtime: Long, spoken_languages: String, status: String,
                       tagline: String, title: String, video: String, vote_average: Int, vote_count: Long,
                       keywords: String, cast: String, crew: String, ratings: String)

object CLimpieza {
  @main def main(): Unit = {
    println("Generando Script tabla PELICULA...")
    val path2Movies = "C:/Users/Personal/Desktop/ProyectoPracticum/pi_movies_complete.csv"
    val file2script = "C:/Users/Personal/Desktop/ProyectoPracticum/script.sql"

    val data2Movie = convert2MovieData(readCSVFile(path2Movies))

    if (generateScript(file2script, data2Movie))
      println("Script generado correctamente")
    else
      println("Parece que no dkjbccabc")
  }

  def readCSVFile(path2File: String): List[Map[String, String]] = {
    implicit object CSVFormatter extends DefaultCSVFormat {
      override val delimiter: Char = ';'
    }
    val reader = CSVReader.open(new File(path2File))
    val dataMap = reader.allWithHeaders()
    reader.close()
    dataMap
  }

  def convert2MovieData(data: List[Map[String, String]]): List[NewComplete] = {
    data.map(row =>
      NewComplete(
        row.getOrElse("adult", ""),
        row.getOrElse("belongs_to_collection", "[]"),
        row.getOrElse("budget", "-1").toLong,
        row.getOrElse("genres", "[]"),
        row.getOrElse("homepage", ""),
        row.getOrElse("id", "-1").toLong,
        row.getOrElse("imd_id", ""),
        row.getOrElse("original_language", ""),
        row.getOrElse("original_title", ""),
        row.getOrElse("overview", ""),
        row.getOrElse("popularity", "-1").toLong,
        row.getOrElse("poster_path", ""),
        row.getOrElse("production_companies", "[]"),
        row.getOrElse("production_countries", "[]"),
        row.getOrElse("release_date", ""),
        row.getOrElse("revenue", "-1").toLong,
        row.getOrElse("runtime", "-1").toLong,
        row.getOrElse("spoken_languages", "[]"),
        row.getOrElse("status", ""),
        row.getOrElse("tagline", ""),
        row.getOrElse("title", ""),
        row.getOrElse("video", ""),
        row.getOrElse("vote_average", "-1").toInt,
        row.getOrElse("vote_count", "-1").toLong,
        row.getOrElse("keywords", "[]"),
        row.getOrElse("cast", "[]"),
        row.getOrElse("crew", "[]"),
        row.getOrElse("ratings", "[]")
      )
    )
  }

  def escapeMySQLString(input: String): String =
    input
      .replace("\\", "\\\\")
      .replace("'", "''")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")

  def generateScript(path2Script: String, data: List[NewComplete]): Boolean = {
    def generateINSERT(row: NewComplete): String = {
      s"""INSERT INTO PELICULA (
        adult, belongs_to_collection, budget, genres, homepage, id, imd_id, original_language, original_title, overview,
        popularity, poster_path, production_companies, production_countries, release_date, revenue, runtime, spoken_languages,
        status, tagline, title, video, vote_average, vote_count, keywords, cast, crew, ratings
      ) VALUES (
        '${escapeMySQLString(row.adult)}', '${escapeMySQLString(row.belongs_to_collection)}', ${row.budget}, '${escapeMySQLString(row.genres)}',
        '${escapeMySQLString(row.homepage)}', ${row.id}, '${escapeMySQLString(row.imd_id)}', '${escapeMySQLString(row.original_language)}',
        '${escapeMySQLString(row.original_title)}', '${escapeMySQLString(row.overview)}', ${row.popularity}, '${escapeMySQLString(row.poster_path)}',
        '${escapeMySQLString(row.production_companies)}', '${escapeMySQLString(row.production_countries)}', '${escapeMySQLString(row.release_date)}',
        ${row.revenue}, ${row.runtime}, '${escapeMySQLString(row.spoken_languages)}', '${escapeMySQLString(row.status)}',
        '${escapeMySQLString(row.tagline)}', '${escapeMySQLString(row.title)}', '${escapeMySQLString(row.video)}',
        ${row.vote_average}, ${row.vote_count}, '${escapeMySQLString(row.keywords)}', '${escapeMySQLString(row.cast)}',
        '${escapeMySQLString(row.crew)}', '${escapeMySQLString(row.ratings)}'
      );"""
    }

    try {
      val file = new BufferedWriter(new FileWriter(path2Script))
      data.foreach { row =>
        file.write(generateINSERT(row))
        file.newLine()
      }
      file.close()
      true
    } catch {
      case e: Exception =>
        println(s"Error encontrado: ${e.getMessage}")
        false
    }
  }
}