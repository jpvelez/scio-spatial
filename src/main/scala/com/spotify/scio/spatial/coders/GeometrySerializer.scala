package com.spotify.scio.spatial.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.KSerializer
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequence

trait GeometrySerializer extends KSerializer[Geometry] {

  def write(kser: Kryo, out: Output, geom: Geometry): Unit = {
    geom.getCoordinates
//    kser.writeClassAndObject(out, geom.getEnvelope)
    kser.writeClassAndObject(out, geom.getCoordinates)
    kser.writeClassAndObject(out, geom.getSRID)
    kser.writeClassAndObject(out, geom.getPrecisionModel)
    println("writing!")
//    kser.writeClassAndObject(out, geom.getUserData)
  }


  override def read(kser: Kryo, in: Input, cls: Class[Geometry]): Geometry = {
//    val env = kser.readClassAndObject(in).asInstanceOf[Envelope]
    val coords = kser.readClassAndObject(in).asInstanceOf[Array[Coordinate]]
    val srid = kser.readClassAndObject(in).asInstanceOf[Int]
    val precisionModel = kser.readClassAndObject(in).asInstanceOf[PrecisionModel]
    val geometryFactory = new GeometryFactory(precisionModel, srid)
    println("reading!")
    createGeom(geometryFactory, coords)
     }

  def createGeom(factory: GeometryFactory, coords: Array[Coordinate]): Geometry

}

class PolygonSerializer extends GeometrySerializer {
  override def createGeom(factory: GeometryFactory, coords: Array[Coordinate]): Polygon = {
    factory.createPolygon(coords)
  }
}

class PointSerializer extends GeometrySerializer {
  override def createGeom(factory: GeometryFactory, coords: Array[Coordinate]): Point = {
    val coordSeq = new CoordinateArraySequence(coords)
    factory.createPoint(coordSeq)
  }
}


