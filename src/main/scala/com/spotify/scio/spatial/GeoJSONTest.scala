package com.spotify.scio.spatial

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.spatial.SpatialConverters._

import scala.concurrent.duration.Duration

object GeoJSONTest {

  def main(cmdLineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdLineArgs)
    val lines = sc.geoJSONFile(args("input")).map(_.getGeometryType).materialize
    sc.close()
    lines.waitForResult(Duration.Inf).value.foreach(println)
  }
}
