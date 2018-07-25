package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.spotify.scio.transforms.DoFnWithResource
import com.spotify.scio.values.SCollection
import org.locationtech.jts.geom.{Geometry, Point}
import org.wololo.jts2geojson.GeoJSONReader

import scala.reflect.ClassTag
import scala.util.Try

object SpatialConverters {

  implicit class SpatialScioContext(sc: ScioContext) {

    // TODO make proper pipeline test
    // TODO add serializers for point, check that sink works there too. write test.
    // TODO check that it fails if you have polygons but expect points, and vice versa.
    // TODO if it does fail, implement the either and update the test. write test.
    // TODO expect behavior: should only container line separated features of the same type. if
    // you ask for the same type, you get corresponding jts types. if you ask for a different type,
    // you get a runtime exception, or an a right-valued either.
    // TODO check if jts2geojson or jts.io can cast directly to the right geomtype?
    // TODO add serializers linear ring, line string, and multi variants.
    // TODO deal with geojson that has feature collections.

    def geoJSONFile[T <: Geometry: ClassTag](path: String): SCollection[T] = {
//    SCollection[Either[Throwable, T]] = {
      lazy val reader = new GeoJSONReader
      sc.textFile(path).map { l => reader.read(l).asInstanceOf[T] }
//      sc.textFile(path).map { l => Try(reader.read(l).asInstanceOf[T]).toEither }
    }
  }
}

