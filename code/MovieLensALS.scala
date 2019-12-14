package task


import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object MovieLensALS {
  
  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("Time4").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("file:/home/student/Downloads/ml-100k/u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    ratings.first()
    val model = ALS.train(ratings, 50, 10, 0.01)
    val predictedRating = model.predict(789, 123)
    val userID = 789
    val K = 10
    val topKRecs = model.recommendProducts(userID, K)
    println("============================")
    println(topKRecs.mkString("\n"))
    println("============================")
    val movies = sc.textFile("file:/home/student/Downloads/ml-100k/u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    println("============================")
    println(moviesForUser.size)
    println("============================")
    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

  }
  
}