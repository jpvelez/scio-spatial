package com.spotify.scio.spatial.coders

import com.esotericsoftware.kryo.Kryo
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.chill.{AllScalaRegistrar, IKryoRegistrar}
import org.locationtech.jts.geom.{Point, Polygon}

@KryoRegistrar
class GeometryKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    val reg = new AllScalaRegistrar
    reg(k)

    println("registering!")
    k.register(classOf[Polygon], new PolygonSerializer)
    k.register(classOf[Point], new PointSerializer)
  }
}
