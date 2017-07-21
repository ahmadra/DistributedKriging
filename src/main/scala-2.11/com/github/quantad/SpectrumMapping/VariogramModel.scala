package com.github.quantad.SpectrumMapping

/**
  * Created by hduser on 10/7/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix,MatrixEntry}
import org.apache.spark.mllib.linalg.Vectors
import java.io._

import scala.sys.process.Process
import com.github.quantad.SpectrumMapping.ProgramUtils._

class VariogramModel {
  def buildModel(sc:SparkContext, reqUrls: Array[String]): Unit = {
    val data = sc.textFile(reqUrls(0))
    val points = data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    val indexed = points.zipWithIndex()
    val indexedData = indexed.map{case (value,index) => (index,value)}
    indexedData.saveAsObjectFile(reqUrls(4))
    val pairedSamples = indexedData.cartesian(indexedData)
    val dist = pairedSamples.map{case (x,y) => ((x,y),distance(x._2,y._2))}
    val dist1 = dist.map{case ((x,y),z) => ((x._1,y._1),z)}

    val maxKey = dist1.max()(new Ordering[((Long, Long), Double)]() {
      override def compare(x: ((Long, Long), Double), y: ((Long, Long), Double)): Int =
        Ordering[Double].compare(x._2, y._2)})
    val numIntervals = 30
    val offset = (maxKey._2.toDouble/numIntervals.toDouble)
    val binPars = sc.broadcast(offset)
    val semi = dist.map{case (x,y) => ((y/binPars.value).toInt, (x._1._2(2), x._2._2(2)))}.groupByKey()
    val semi1 = semi.map{case (x,y) => (x, semivar(y))}.filter{case (x,y) => x!=0}
    val lagsarr:Array[(Int, Double)] = semi1.collect()

    val filetowrite = reqUrls(1)
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filetowrite)))
    for (x <- lagsarr) {writer.write(x.toString())
      writer.newLine()}
    writer.close()
    val pb = Process("python" + " " + reqUrls(3) + " " + reqUrls(1) + " " + reqUrls(2))
    val exitCode = pb.!

    val params = getParams(reqUrls(2))
    val distParam = sc.broadcast(params)
    val numObs = data.count().toInt
    val distNum = sc.broadcast(numObs)

    val varmat = dist.map{case ((x,y),z) => ((x,y),z,computeVar(z, distParam.value))}
    val lhsmatent: RDD[MatrixEntry] = varmat.map{case ((x,y),z,v) => MatrixEntry(x._1,y._1,v)}
    val range1 = 0 to numObs
    val range2 = numObs to numObs
    val rowcol1 = sc.parallelize(for (a_ <- range1; b_ <- range2) yield (a_, b_)).map{case (x,y) => if(x==y) MatrixEntry(x,y,0.toDouble) else MatrixEntry(x,y,1.toDouble)}
    val rowcol2 = sc.parallelize(for (a_ <- range2; b_ <- range1) yield (a_, b_)).filter{case (x,y) => x!=y}.map{case (x,y) => MatrixEntry(x,y,1.toDouble)}
    val lhsrdd = sc.union(lhsmatent,rowcol1,rowcol2)
    val lhsmat = new CoordinateMatrix(lhsrdd)
    val AMat = lhsmat.toIndexedRowMatrix()
    AMat.rows.saveAsObjectFile(reqUrls(5))

  }
}