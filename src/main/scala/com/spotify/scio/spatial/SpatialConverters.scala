package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.vividsolutions.jts.geom.Geometry
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.formatMapper.FormatMapper

import scala.concurrent.duration.Duration

object SpatialConverters {

  implicit class SpatialScioContext(sc: ScioContext) {

    def geoJSONFile[T <: Geometry](path: String) = {
      val fm = new FormatMapper(FileDataSplitter.GEOJSON, false)
      sc.textFile(path).map(fm.readGeoJSON)
    }

  }
}

