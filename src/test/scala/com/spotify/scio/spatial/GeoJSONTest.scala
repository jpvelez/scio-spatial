package com.spotify.scio.spatial

import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.spatial.SpatialConverters._
import com.spotify.scio.testing.PipelineSpec
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.wololo.jts2geojson.GeoJSONReader

import scala.concurrent.duration.Duration


class GeoJSONTest extends PipelineSpec {

//  def main(cmdLineArgs: Array[String]): Unit = {
//
//    val (sc, args) = ContextAndArgs(cmdLineArgs)
//    val lines = sc.geoJSONFile[Polygon](args("input")).map(_.getArea).materialize
//    sc.close()
//    lines.waitForResult(Duration.Inf).value.foreach(println)
//  }

  val reader = new GeoJSONReader

  val geoJSONPolygons =
    "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]," +
      "[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992]," +
      "[170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}\n" +
      "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]" +
      ",[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992]," +
      "[170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}\n" +
      "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]," +
      "[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992]," +
      "[170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}\n" +
      "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]," +
      "[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992],[" +
      "170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}\n" +
      "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]," +
      "[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992]," +
      "[170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}\n" +
      "{\"type\":\"Polygon\",\"coordinates\":[[[170.199999999997,-24.1999999999992]," +
      "[170.417999999997,-24.1999999999992],[170.417999999997,-24.7519999999992]," +
      "[170.199999999997,-24.7519999999992],[170.199999999997,-24.1999999999992]]]}"

  val geoJSONFeatureCollection =
    "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    {\"type\":\"Feature\",\"properties\":{\"objectid\":\"491\",\"url\":\"http://web.mta.info/nyct/service/\",\"name\":\"6th Ave & 16th St at NE corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[-73.99563200053089,40.738678000646054]}},\n    {\"type\":\"Feature\",\"properties\":{\"objectid\":\"492\",\"url\":\"http://web.mta.info/nyct/service/\",\"name\":\"6th Ave & 16th St at SE corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[-73.99568500052408,40.738529001083776]}},\n    {\"type\":\"Feature\",\"properties\":{\"objectid\":\"493\",\"url\":\"http://web.mta.info/nyct/service/\",\"name\":\"6th Ave & 16th St at NW corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[-73.99602300002492,40.73883800075859]}},\n    {\"type\":\"Feature\",\"properties\":{\"objectid\":\"494\",\"url\":\"http://web.mta.info/nyct/service/\",\"name\":\"6th Ave & 16th St at SW corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[-73.99612100013594,40.73871800068385]}}\n  ]\n}\n    "

  def geoJSONToJTS(geoJSON: String): Array[Geometry] = {
    geoJSON.split("\n").map(reader.read)
  }

  "geoJSONFile method" should "read newline-separated geoJSON polygons as JTS Polygons" in {
    val expected = geoJSONToJTS(geoJSONPolygons).map(_.asInstanceOf[Polygon])
    val sc = ScioContext()
    val geoms = sc.geoJSONFile[Polygon]("./src/test/resources/polygons.geojson")
    geoms should containInAnyOrder(expected)
    sc.close()
  }

  ""



}
