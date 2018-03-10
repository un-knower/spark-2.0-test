package ml.vectors

import org.apache.spark.ml.linalg.Vectors

/**
  * Created by yxl on 2017/12/4.
  */
object TestVector {

    def main(args: Array[String]): Unit = {

        val vectors = Seq(
            Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,7.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,5.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(4.0,7.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(2.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(9.0,5.0,7.0)),
            Vectors.sparse(3,Array(0,1,2),Array(2.0,5.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(8.0,4.0,9.0)),
            Vectors.sparse(3,Array(0,1,2),Array(3.0,6.0,2.0)),
            Vectors.sparse(3,Array(0,1,2),Array(5.0,9.0,2.0))
        )

        println(vectors)

        val df = vectors.map(Tuple1.apply)

        println(df)


    }

}
