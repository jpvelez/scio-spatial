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


    // TODO ANALYZE GEOSPARK
    // Partitioning: you partition with a partitioner. Partitioners break up the space into spatial partitions,
    // and then decide geometry lies inside of one partition (in the case of points) or maybe several (in the case of po
    // polygons). Then you use Spark's partitionBy to basically shuffle each geometry to a node that houses all the
    // geometries that fall ~within~ that spatial partition. For as yet unknown reasons, every partition requires a some
    // bounding box of all the data (the latter makes sense) and/or some sample of the data (presumably to set the partitions).
    // Both require doing a full pass of the data: to get the spatial extent, and to get the approximate total count
    // of elements, from which you can figure out how many samples you need, and then sample.

    // Index building: using mapPartitions, you iterate through the geometries in every partition (spatially paritioned
    // or random), and insert them into one of many indexes in JTS. You insert their envelope, and the geom itself.
    // Then, you get an RDD of Indexes. The indexes now house all the points. This RDD is typically persisted.
    // Now, you can query the index in different ways: KNN to a point, points within a bounding box, distance queries,
    // etc.

    // Scio Implementation.
    // 1. Sketch (first pass): Do first pass over the data, using Algebird aggregators - get max x, min x, max y, min y, and approximate
    // count in one pass.
    // 2. Sample (second pass): If you need to sample, do a second pass over the data to sample. Scio has sampling methods.
    // 3. Build partitioner: build the partitioner object using sketches and maybe sample.
    // 4. Partition (third pass): shuffle each geometry to the appropriate partition using groupby and the partitioner.
    // 5. Build index: insert each element into the index, and save the indexes to GCS? Or somehow pass them with sideinput?
    // 6. Query: now that you have the index, look up the bounding box (range query), point (knn), or SOMEHOW
    // go through the elements of the left hand table and do lookups in the indexes. Do they have to end up on the same
    // node too?
    // TODO HOW DOES GEOSPARK HANDLE THIS CASE? KEEP RESEARCHING THIS, AND ALSO CLARIFY THE SCIO HANDOFFS, UNTIL YOURE SURE ITS FEASIBLE.


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

