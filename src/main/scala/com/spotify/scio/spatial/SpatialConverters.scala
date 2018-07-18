package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.locationtech.jts.geom.Geometry
import org.wololo.jts2geojson.GeoJSONReader

object SpatialConverters {

  implicit class SpatialScioContext(sc: ScioContext) {

    // KryoAtomicCoder serializes JTS Geometry instances to different values. Must be one of these:
    // http://javadox.com/com.vividsolutions/jts/1.13/serialized-form.html
    // TODO replicate the issue kicked up by org.apache.beam.runners.direct.ImmutabilityCheckingBundleFactory
    // Then, identify the problem field. Then, write a KSerializer that deals with this field,
    // and register it with an IKryoRegistar once you test the KSerializer. byte arrays should be
    // the same...
    def geoJSONFile(path: String): SCollection[Geometry] = {
      lazy val reader = new GeoJSONReader
      sc.textFile(path).map(reader.read)
    }

  }
}

