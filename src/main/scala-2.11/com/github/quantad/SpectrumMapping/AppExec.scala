package com.github.quantad.SpectrumMapping

/**
  * Created by hduser on 10/7/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

import scala.sys.process.Process



object AppExec {
  def main(args: Array[String]): Unit = {

    var PATH = new java.io.File(".").getCanonicalPath
    PATH = PATH + "/src/main/scala-2.11/com/github/quantad/SpectrumMapping"
    val modelsPath = PATH + "/models"

    val dataFile: String = "hdfs://localhost:54310/usrp/realdata.txt"
    val empsemFile: String = modelsPath + "/empsem7.txt"
    val fitsemFile: String = modelsPath + "/fitsem7.txt"
    val fitscriptFile: String = PATH + "/simple.py"
    val indexedFile: String = "hdfs://localhost:54310/usrp/indexedFile.txt"
    val outputFile: String = "hdfs://localhost:54310/usrp/modelFile.txt"
    val predFile: String = "hdfs://localhost:54310/usrp/predata.txt"
    def reqFiles: Array[String] = Array(dataFile, empsemFile, fitsemFile, fitscriptFile, indexedFile, outputFile, predFile)

    val conf = new SparkConf().setAppName("SimpApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    useModel(sc, reqFiles)
  }

  def buildModel(sc:SparkContext,reqFiles:Array[String]): Unit = {
    val pb1 = Process("/usr/local/hadoop/bin/hadoop" + " " + "fs" + " " + "-rmr" + " " + reqFiles(4))
    val exitCode1 = pb1.!
    val pb2 = Process("/usr/local/hadoop/bin/hadoop" + " " + "fs" + " " + "-rmr" + " " + reqFiles(5))
    val exitCode2 = pb2.!
    new File(reqFiles(1)).delete()
    new File(reqFiles(2)).delete()

    new VariogramModel().buildModel(sc, reqFiles)
  }

  def useModel(sc:SparkContext, reqFiles: Array[String]): Unit = {
    new KrigingInterpolation(sc,reqFiles).doKriging()
  }
}
