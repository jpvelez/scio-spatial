package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, Coordinate}
import org.wololo.geojson._
import org.wololo.geojson
import org.wololo.jts2geojson.GeoJSONReader

import scala.reflect.ClassTag

object SpatialConverters {

  implicit class SpatialScioContext(sc: ScioContext) {

    // TODO IMPORT FAILURES
    // check that it fails if you have polygons but expect points, and vice versa.
    // if it does fail, implement the either and update the test. write test.
    // expect behavior: should only container line separated features of the same type. if
    // you ask for the same type, you get corresponding jts types. if you ask for a different type,
    // you get a runtime exception, or an a right-valued either.

    // TODO IMPORT ALL GEOJSON/JTS DATA TYPES
    // add serializers for linear ring, line string, and multi variants. write test for imports.
    // Test one real-world geojson data.


    def geoJSONFile[T <: Geometry: ClassTag](path: String): SCollection[T] = {
      lazy val reader = new GeoJSONReader
      sc.textFile(path)
        .map[GeoJSON](GeoJSONFactory.create)
        .flatMap[geojson.Geometry] {
          case fc: FeatureCollection => fc.getFeatures.map(_.getGeometry)
          case f: Feature => Some(f.getGeometry)
          case gc: GeometryCollection => gc.getGeometries
          case geom: geojson.Geometry => Some(geom)
        }
        .map( geom => reader.read(geom).asInstanceOf[T] )
    }
  }

  implicit class SpatialSCollection[T <: Geometry](self: SCollection[T]) {

    def rangeQuery(x1: Double, x2: Double, y1: Double, y2: Double): SCollection[T] = {
      val boundingBox = new GeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2))
      self.filter( geom => geom within boundingBox )
    }

    def knnQuery(x: Double, y: Double)(k: Int = 5): SCollection[(Double, T)] = {
      val point = new GeometryFactory().createPoint(new Coordinate(x, y))
      self.map( geom => (geom distance point, geom) )
        .top(k)(Ordering.by((distance: (Double, T)) => distance._1).reverse) // Return k-nearest.
        .flatMap[(Double, T)](identity(_))
    }

    def distanceJoin[U <: Geometry: ClassTag](other: SCollection[U])
                                   (withinDistance: Double) : SCollection[(T, U)] = {
      self.cross(other)  // Right side must be tiny.
          .filter { case (l, r) => l.isWithinDistance(r, withinDistance) }
    }

    def spatialJoin[U <: Geometry: ClassTag](other: SCollection[U]): SCollection[(T, U)] = {
      self.cross(other)
        .filter { case (l, r) => l.covers(r) } // Many other spatial predicates. 1 op per predicate?
    }
  }
}

