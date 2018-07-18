package com.spotify.scio.spatial

import com.twitter.chill.KSerializer
import org.locationtech.jts.geom._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory

class GeometrySerializer extends KSerializer[Geometry] {

  def write(kser: Kryo, out: Output, geom: Geometry): Unit = {
//    kser.writeClassAndObject(out, geom.getEnvelope)
    kser.writeClassAndObject(out, geom.getCoordinates)
    kser.writeClassAndObject(out, geom.getSRID)
    kser.writeClassAndObject(out, geom.getPrecisionModel)
//    kser.writeClassAndObject(out, geom.getUserData)
  }


  override def read(kser: Kryo, in: Input, cls: Class[Geometry]): Geometry = {
//    val env = kser.readClassAndObject(in).asInstanceOf[Envelope]
    val coords = kser.readClassAndObject(in).asInstanceOf[Array[Coordinate]]
    val srid = kser.readClassAndObject(in).asInstanceOf[Int]
    val precisionModel = kser.readClassAndObject(in).asInstanceOf[PrecisionModel]
    val geometryFactory = new GeometryFactory(precisionModel, srid)
    geometryFactory.createPolygon(coords)
     }
}

