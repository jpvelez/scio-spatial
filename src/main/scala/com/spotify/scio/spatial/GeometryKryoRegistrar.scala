package com.spotify.scio.spatial

import com.esotericsoftware.kryo.Kryo
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.chill.{AllScalaRegistrar, IKryoRegistrar}
import org.locationtech.jts.geom.Geometry

@KryoRegistrar
class GeometryKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    val reg = new AllScalaRegistrar
    reg(k)

    k.register(classOf[Geometry], new GeometrySerializer)
  }
}
