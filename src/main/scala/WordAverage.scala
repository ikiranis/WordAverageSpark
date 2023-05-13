import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object WordAverage {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordAverage")
        val sc = new SparkContext(sparkConf)     // create spark context

        val inputFile = "input/SherlockHolmes.txt"
        val outputDir = "output"

        // Διάβασμα του αρχείου και αφαίρεση σημείων στίξης. Μετατροπή όλων των χαρακτήρων σε πεζά.
        val txtFile = sc.textFile(inputFile)
            .map(_.replaceAll("[^A-Za-z0-9]+", " "))
            .map(_.toLowerCase)

        // Παίρνει το πρώτο γράμμα κάθε λέξης και το μήκος της λέξης, στην μορφή (firstLetter, wordLength)
        val firstWordLetters = txtFile.flatMap(line => line.split(" "))
            // Φιλτράρισμα των λέξεων που αρχίζουν με αριθμό ή είναι κενές
            .filter(word => !word.matches("^[0-9].*") && word.nonEmpty)
            // Δημιουργία ζευγών με το πρώτο γράμμα της λέξης και το μέγεθος της σε χαρακτήρες (firstLetter, word length)
            .map(word => (word.charAt(0), word.length))

        // Υπολογισμός του μέσου όρου του μήκους των λέξεων για κάθε γράμμα
        val averageLengths = firstWordLetters
            .combineByKey(
                (valueCount) => (valueCount, 1), // Δημιουργία του combiner. Αρχικοποίηση της τιμής του κάθε κλειδιού
                (acc: (Int, Int), valueCount) => (acc._1 + valueCount, acc._2 + 1), // Υπολογισμός του αθροίσματος των μηκών των λέξεων και αύξηση του μετρητή
                (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // Συνένωση των αθροισμάτων και των μετρητών από τα partitions
            ) // Το αποτέλεσμα είναι στη μορφή (firstLetter, (sumOfLengths, countOfWords))
            .mapValues(sumAndCount => f"${sumAndCount._1.toDouble / sumAndCount._2.toDouble}%.1f") // Υπολογισμός μέσου όρου, με βάση τα sumOfLengths, countOfWords

        // Τελική ταξινόμηση και εξαγωγή σε αρχείο
        averageLengths.sortByKey().saveAsTextFile(outputDir)

        sc.stop()
    }
}
