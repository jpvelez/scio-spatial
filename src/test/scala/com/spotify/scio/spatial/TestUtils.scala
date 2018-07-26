package com.spotify.scio.spatial

import org.locationtech.jts.geom.Geometry
import org.wololo.geojson.{FeatureCollection, GeoJSONFactory}
import org.wololo.jts2geojson.GeoJSONReader

import scala.reflect.ClassTag

object TestUtils {

  val reader = new GeoJSONReader

  def geoJSONToJTS[T <: Geometry: ClassTag](geoJSON: String, delimited: Boolean = false): Array[T] = {
    val geoms = if (delimited) {
      geoJSON.split("\n").map(reader.read)
    } else {
      val geoms = GeoJSONFactory.create(geoJSON).asInstanceOf[FeatureCollection]
        .getFeatures.map(_.getGeometry)
      geoms.map(reader.read)
    }
    geoms.map(_.asInstanceOf[T])
  }


}
