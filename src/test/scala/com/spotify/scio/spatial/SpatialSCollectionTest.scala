package com.spotify.scio.spatial

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.spatial.SpatialConverters._
import com.spotify.scio.spatial.TestUtils.geoJSONToJTS
import org.locationtech.jts.geom.Point


class SpatialSCollectionTest extends PipelineSpec {

  val geoJSONPoints = "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    " +
    "{\"type\":\"Feature\",\"properties\":{\"objectid\":\"491\",\"url\":\"http://web.mta.info/ny" +
    "ct/service/\",\"name\":\"6th Ave & 16th St at NE corner\",\"line\":\"F-L-M-1-2-3\"},\"geome" +
    "try\":{\"type\":\"Point\",\"coordinates\":[-73.99563200053089,40.738678000646054]}},\n    {" +
    "\"type\":\"Feature\",\"properties\":{\"objectid\":\"492\",\"url\":\"http://web.mta.info/nyc" +
    "t/service/\",\"name\":\"6th Ave & 16th St at SE corner\",\"line\":\"F-L-M-1-2-3\"},\"geomet" +
    "ry\":{\"type\":\"Point\",\"coordinates\":[-73.99568500052408,40.738529001083776]}},\n    {" +
    "\"type\":\"Feature\",\"properties\":{\"objectid\":\"493\",\"url\":\"http://web.mta.info/nyc" +
    "t/service/\",\"name\":\"6th Ave & 16th St at NW corner\",\"line\":\"F-L-M-1-2-3\"},\"geomet" +
    "ry\":{\"type\":\"Point\",\"coordinates\":[-73.99602300002492,40.73883800075859]}},\n    {\"" +
    "type\":\"Feature\",\"properties\":{\"objectid\":\"494\",\"url\":\"http://web.mta.info/nyct/" +
    "service/\",\"name\":\"6th Ave & 16th St at SW corner\",\"line\":\"F-L-M-1-2-3\"},\"geometry" +
    "\":{\"type\":\"Point\",\"coordinates\":[-73.99612100013594,40.73871800068385]}}\n  ]\n}"

  "A SpatialSCollection's range query method" should
    "return all points within in a bounding box" in {

    val sc = ScioContext()
    // x = longitude, y = latitude.
    val expected = geoJSONToJTS[Point](geoJSONPoints)
    val points = sc.geoJSONFile[Point]("./src/test/resources/nyc_subway_stops.geojson")
      .rangeQuery(-73.99633973836897, -73.99545192718506, 40.73902875849861, 40.738417038547745)
    points should containInAnyOrder(expected)
    sc.close()
  }

}
