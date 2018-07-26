package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, Polygon}
import org.wololo.geojson._
import org.wololo.geojson
import org.wololo.jts2geojson.GeoJSONReader

import scala.reflect.ClassTag

object SpatialConverters {

  implicit class SpatialScioContext(sc: ScioContext) {

    // TODO check that it fails if you have polygons but expect points, and vice versa.
    // TODO if it does fail, implement the either and update the test. write test.
    // TODO expect behavior: should only container line separated features of the same type. if
    // you ask for the same type, you get corresponding jts types. if you ask for a different type,
    // you get a runtime exception, or an a right-valued either.
    // TODO check if jts2geojson or jts.io can cast directly to the right geomtype?
    // TODO add serializers linear ring, line string, and multi variants.


    def geoJSONFile[T <: Geometry: ClassTag](path: String): SCollection[T] = {
      lazy val reader = new GeoJSONReader
      sc.textFile(path)
        .flatMap[geojson.Geometry] {
        GeoJSONFactory.create(_) match {
          case fc: FeatureCollection => fc.getFeatures.map(_.getGeometry)
          case f: Feature => Some(f.getGeometry)
          case gc: GeometryCollection => gc.getGeometries
          case geom: geojson.Geometry => Some(geom)
        }
      }
        .map( geom => reader.read(geom).asInstanceOf[T] )
    }
  }


  implicit class SpatialSCollection[T <: Geometry](self: SCollection[T]) {

    def rangeQuery(x1: Double, x2: Double, y1: Double, y2: Double): SCollection[T] = {
      val boundingBox = new GeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2))
      self.filter( geom => geom.within(boundingBox) )
    }
  }
}

