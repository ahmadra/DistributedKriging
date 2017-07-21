package com.github.quantad.SpectrumMapping

/**
  * Created by hduser on 10/7/16.
  */

import java.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.io.Source


object ProgramUtils {

  def distance(p1:Vector, p2:Vector): Double = {
    var gap:Double = 0
    gap =  math.sqrt(math.pow(p1(0) - p2(0), 2) + math.pow(p1(1) - p2(1), 2))
    gap
  }

  def semivar(grouped: Iterable[(Double, Double)]): Double = {
    val itlen = grouped.size
    (((grouped.map{case (i,j) => math.pow(i - j, 2)}).sum)/itlen.toDouble)/(itlen.toDouble)
  }

  def computeVar(dist:Double, distParam:Array[Double]) = {
    val ff = (distParam(0) - distParam(2))*math.exp((-3.0*dist)/distParam(1))
    ff
  }

  def getParams(paramFile: String): Array[Double] = {
    val filename = paramFile
    var arr = new Array[Double](3)
    var ind = 0
    for (line <- Source.fromFile(filename).getLines) {
      arr(ind) = line.toDouble
      ind = ind + 1
    }
    arr
  }

  def randomizeDenseVector(dv:Vector, randParams:Broadcast[Array[Double]], rand:Random): Vector = {
    var loopCond = 1.toInt
    var x: Vector = Vectors.dense(1.0, 1.0)
    while (loopCond == 1) {
      val dim1 = randParams.value(1)-randParams.value(0)
      val dim2 = randParams.value(3)-randParams.value(2)
      val rad: Double = math.min(dim1, dim2)/4.0
      val t = 2*math.Pi*(rand.nextDouble())
      val r = rad * math.sqrt(rand.nextDouble())
      val xrand = r * math.cos(t)
      val yrand = r * math.sin(t)
      if (dv(0)+xrand >= randParams.value(0) &&
        dv(0)+xrand <= randParams.value(1) &&
        dv(1)+yrand >= randParams.value(2) &&
        dv(1)+yrand <= randParams.value(3)){
        x = Vectors.dense(dv(0)+xrand, dv(1)+yrand)
        loopCond = 0
      }
    }
    x
  }
}
