package com.github.quantad.SpectrumMapping

/**
  * Created by hduser on 10/7/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector,Vectors,Vector}
import org.apache.spark.broadcast._

import com.github.quantad.SpectrumMapping.ProgramUtils._
import java.util.Random
import math._
import scala.util.control.Breaks._
import java.lang.String._


class KrigingInterpolation (sc:SparkContext, reqUrls:Array[String]) extends java.io.Serializable{

  def loadData(): RDD[Vector] = {
    val predData = sc.textFile(reqUrls(6)).map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    predData
  }

  def obtainWeights(predData:RDD[Vector]) = {

    val indexedData: RDD[(Long,DenseVector)] = sc.objectFile(reqUrls(4))
    val numObs = indexedData.count().toInt
    val distParam = sc.broadcast((getParams(reqUrls(2)),numObs))
    val numPre = predData.count().toInt
    val rhs = predData.zipWithIndex().map{case (x,y) => (y,x)}.cartesian(indexedData)
    val rhsdist = rhs.map{case (x,y) => ((x,y),distance(x._2,y._2))}
    val rhsvar: RDD[((Long,Long),Double)] = rhsdist.map{case ((x,y),z) => ((x._1,y._1),
      computeVar(z, distParam.value._1))}

    val r1 = 0 to 0
    val r2 = 0 to (numPre-1)
    val rowcol3: RDD[((Long,Long),Double)] = sc.parallelize(for (a_ <- r1; b_ <- r2) yield (a_, b_)).
      map{case (x,y) => ((y.toLong,distParam.value._2.toLong),1.toDouble)}
    val rhsvarmat = sc.union(rhsvar,rowcol3).sortByKey().map{case (x,y) => (x._1,y)}.groupByKey()
    rhsvarmat.collect().foreach(println)
    val vecMat = rhsvarmat.map{case (x,y) =>
      (x,new breeze.linalg.DenseMatrix(distParam.value._2 + 1, 1, y.toArray))}
    val aMatRows: RDD[IndexedRow] = sc.objectFile(reqUrls(5))
    val AMat: IndexedRowMatrix = new IndexedRowMatrix(aMatRows)
    val AMat1 = AMat.toRowMatrix()

    val AMat2 = new breeze.linalg.DenseMatrix(AMat1.numRows().toInt,AMat1.numCols().toInt,
      AMat1.rows.collect.flatMap(x => x.toArray))
    val distInv = sc.broadcast(AMat2)
    val mulMat = vecMat.map{case (x,y) => ((x,y),(distInv.value \ y).toArray)}
    (mulMat, indexedData, distParam)
  }

  def mapValues(weights:RDD[((Long,breeze.linalg.DenseMatrix[Double]),Array[Double])],
                indexedData: RDD[(Long,DenseVector)],
                distParam:Broadcast[(Array[Double],Int)]): Array[(Long,Double)] = {
    val indZ = weights.flatMap { case ((index,vec), arr) => arr.map{case x => (index, x)} }.
      zipWithIndex().map(_.swap)
    val indZ1 = indZ.map{case (x,y) => if (x > distParam.value._2) (x%(distParam.value._2 + 1),y) else (x,y)}
    val indZ2 = indZ1.join(indexedData).map{case (x,y) => (y._1._1,y._1._2*y._2(2))}.
      reduceByKey((accum: Double, n: Double) => (accum + n))
    indZ2.collect()
  }

  def doKriging(): Unit ={
    val predictionData = loadData()
    val weights = obtainWeights(predictionData)
    val predictions = mapValues(weights._1, weights._2, weights._3)
    predictions.foreach(println)
  }
}
