package com.spotify.scio.spatial

import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.spatial.SpatialConverters._
import com.spotify.scio.testing.PipelineSpec
import org.locationtech.jts.geom._
import org.wololo.geojson.{FeatureCollection, GeoJSONFactory}
import org.wololo.jts2geojson.GeoJSONReader

import scala.concurrent.duration.Duration


class GeoJSONTest extends PipelineSpec {

  val reader = new GeoJSONReader

  def geoJSONToJTS(geoJSON: String, delimited: Boolean = false): Array[Geometry] = {
    if (delimited) {
      geoJSON.split("\n").map(reader.read)
    }
    else {
      val geoms = GeoJSONFactory.create(geoJSON).asInstanceOf[FeatureCollection]
        .getFeatures.map(_.getGeometry)
      geoms.map(reader.read)
    }
  }

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

  val geoJSONFeatureCollection = "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    " +
    "{\"type\":\"Feature\",\"properties\":{\"objectid\":\"491\",\"url\":\"http://web.mta.info/ny" +
    "ct/service/\",\"name\":\"6th Ave & 16th St at NE corner\",\"line\":\"F-L-M-1-2-3\"},\"geome" +
    "try\":{\"type\":\"Point\",\"coordinates\":[-73.99563200053089,40.738678000646054]}},\n    {" +
    "\"type\":\"Feature\",\"properties\":{\"objectid\":\"492\",\"url\":\"http://web.mta.info/nyc" +
    "t/service/\",\"name\":\"6th Ave & 16th St at SE corner\",\"line\":\"F-L-M-1-2-3\"},\"geomet" +
    "ry\":{\"type\":\"Point\",\"coordinates\":[-73.99568500052408,40.738529001083776]}},\n    {" +
    "\"type\":\"Feature\",\"properties\":{\"objectid\":\"493\",\"url\":\"http://web.mta.info/nyc" +
    "t/service/\",\"name\":\"6th Ave & 16th St at NW corner\",\"line\":\"F-L-M-1-2-3\"},\"geomet" +
    "ry\":{\"type\":\"Point\",\"coordinates\":[-73.99602300002492,40.73883800075859]}},\n    {\"" +
    "type\":\"Feature\",\"properties\":{\"objectid\":\"494\",\"url\":\"http://web.mta.info/nyct/" +
    "service/\",\"name\":\"6th Ave & 16th St at SW corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry" +
    "\":{\"type\":\"Point\",\"coordinates\":[-73.99612100013594,40.73871800068385]}}\n  ]\n}"


  "geoJSONFile method" should "read newline-separated geoJSON objects containing polygon " +
    "geometries as JTS Polygons" in {
    val expected = geoJSONToJTS(geoJSONPolygons, delimited = true).map(_.asInstanceOf[Polygon])
    val sc = ScioContext()
    val geoms = sc.geoJSONFile[Polygon]("./src/test/resources/polygons.geojson")
    geoms should containInAnyOrder(expected)
    sc.close()
  }

  "geoJSONFile method" should "read point geometries features from a single FeatureCollection " +
    "geoJSON object into JTS Points" in {
    val expected = geoJSONToJTS(geoJSONFeatureCollection).map(_.asInstanceOf[Point])
    val sc = ScioContext()
    val geoms = sc.geoJSONFile[Point]("./src/test/resources/nyc_subway_stops_sample.geojson")
    geoms should containInAnyOrder(expected)
    sc.close()
  }
}
