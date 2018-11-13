import httpretty
from owslib.etree import etree

from aodncore.testlib import BaseTestCase
from aodncore.util import IndexedSet
from aodncore.util.wfs import WfsBroker, get_filter_for_file_url

TEST_GETCAPABILITIES_RESPONSE = httpretty.Response('''<?xml version="1.0" encoding="UTF-8"?>
<WFS_Capabilities version="1.0.0" xmlns="http://www.opengis.net/wfs" xmlns:aodn="aodn" xmlns:imos="imos.mod" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wfs http://geoserver.example.com/geoserver/schemas/wfs/1.0.0/WFS-capabilities.xsd">
  <Service>
    <Name>WFS</Name>
    <Title />
    <Abstract />
    <Keywords />
    <OnlineResource>http://geoserver.example.com/geoserver/wfs</OnlineResource>
    <Fees />
    <AccessConstraints />
  </Service>
  <Capability>
    <Request>
      <GetCapabilities>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=GetCapabilities" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </GetCapabilities>
      <DescribeFeatureType>
        <SchemaDescriptionLanguage>
          <XMLSCHEMA />
        </SchemaDescriptionLanguage>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=DescribeFeatureType" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </DescribeFeatureType>
      <GetFeature>
        <ResultFormat>
          <CSV />
          <KML />
          <GML2 />
          <GML3 />
          <SHAPE-ZIP />
          <JSON />
        </ResultFormat>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=GetFeature" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </GetFeature>
      <Transaction>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=Transaction" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </Transaction>
      <LockFeature>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=LockFeature" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </LockFeature>
      <GetFeatureWithLock>
        <ResultFormat>
          <GML2 />
        </ResultFormat>
        <DCPType>
          <HTTP>
            <Get onlineResource="http://geoserver.example.com/geoserver/wfs?request=GetFeatureWithLock" />
          </HTTP>
        </DCPType>
        <DCPType>
          <HTTP>
            <Post onlineResource="http://geoserver.example.com/geoserver/wfs" />
          </HTTP>
        </DCPType>
      </GetFeatureWithLock>
    </Request>
  </Capability>
  <FeatureTypeList>
    <Operations>
      <Query />
      <Insert />
      <Update />
      <Delete />
      <Lock />
    </Operations>
    <FeatureType>
      <Name>imos:anmn_velocity_timeseries_map</Name>
      <Title>ANMN Current timeseries (map)</Title>
      <Abstract>Time-series observations of current from coastal moorings deployed by the Australian National Mooring Network (ANMN) Facility of IMOS.</Abstract>
      <Keywords>anmn_velocity_timeseries_map, features</Keywords>
      <SRS>EPSG:4326</SRS>
      <LatLongBoundingBox minx="-180.0" miny="-90.0" maxx="180.0" maxy="90.0" />
    </FeatureType>
  </FeatureTypeList>
  <ogc:Filter_Capabilities>
    <ogc:Spatial_Capabilities>
      <ogc:Spatial_Operators>
        <ogc:Disjoint />
        <ogc:Equals />
        <ogc:DWithin />
        <ogc:Beyond />
        <ogc:Intersect />
        <ogc:Touches />
        <ogc:Crosses />
        <ogc:Within />
        <ogc:Contains />
        <ogc:Overlaps />
        <ogc:BBOX />
      </ogc:Spatial_Operators>
    </ogc:Spatial_Capabilities>
    <ogc:Scalar_Capabilities>
      <ogc:Logical_Operators />
      <ogc:Comparison_Operators>
        <ogc:Simple_Comparisons />
        <ogc:Between />
        <ogc:Like />
        <ogc:NullCheck />
      </ogc:Comparison_Operators>
      <ogc:Arithmetic_Operators>
        <ogc:Simple_Arithmetic />
        <ogc:Functions>
          <ogc:Function_Names>
            <ogc:Function_Name nArgs="1">abs</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">abs_2</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">abs_3</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">abs_4</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">acos</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">AddCoverages</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">Affine</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">Aggregate</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Area</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">area2</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">AreaGrid</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">asin</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">atan</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">atan2</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">BandMerge</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">BandSelect</ogc:Function_Name>
            <ogc:Function_Name nArgs="-6">BarnesSurface</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">between</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">boundary</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">boundaryDimension</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Bounds</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">buffer</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">BufferFeatureCollection</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">bufferWithSegments</ogc:Function_Name>
            <ogc:Function_Name nArgs="7">Categorize</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">ceil</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">centroid</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">classify</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">Clip</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">CollectGeometries</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Average</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Bounds</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">Collection_Count</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Max</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Median</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Min</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Nearest</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Sum</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Collection_Unique</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">Concatenate</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">contains</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">Contour</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">convert</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">convexHull</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">cos</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">Count</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">CropCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">crosses</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">dateFormat</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">dateParse</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">densify</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">difference</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">dimension</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">disjoint</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">disjoint3D</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">distance</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">distance3D</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">double2bool</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">endAngle</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">endPoint</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">env</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">envelope</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">EqualInterval</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">equalsExact</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">equalsExactTolerance</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">equalTo</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">exp</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">exteriorRing</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">Feature</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">floor</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">geometry</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">geometryType</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">geomFromWKT</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">geomLength</ogc:Function_Name>
            <ogc:Function_Name nArgs="-3">GeorectifyCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">GetFullCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">getGeometryN</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">getX</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">getY</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">getz</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">GoGoDuck</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">greaterEqualThan</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">greaterThan</ogc:Function_Name>
            <ogc:Function_Name nArgs="-3">Grid</ogc:Function_Name>
            <ogc:Function_Name nArgs="-5">Heatmap</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">id</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">IEEEremainder</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">if_then_else</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">Import</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">in</ogc:Function_Name>
            <ogc:Function_Name nArgs="11">in10</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">in2</ogc:Function_Name>
            <ogc:Function_Name nArgs="4">in3</ogc:Function_Name>
            <ogc:Function_Name nArgs="5">in4</ogc:Function_Name>
            <ogc:Function_Name nArgs="6">in5</ogc:Function_Name>
            <ogc:Function_Name nArgs="7">in6</ogc:Function_Name>
            <ogc:Function_Name nArgs="8">in7</ogc:Function_Name>
            <ogc:Function_Name nArgs="9">in8</ogc:Function_Name>
            <ogc:Function_Name nArgs="10">in9</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">InclusionFeatureCollection</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">int2bbool</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">int2ddouble</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">interiorPoint</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">interiorRingN</ogc:Function_Name>
            <ogc:Function_Name nArgs="-5">Interpolate</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">intersection</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">IntersectionFeatureCollection</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">intersects</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">intersects3D</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isClosed</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">isCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isEmpty</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isInstanceOf</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">isLike</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isNull</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">isometric</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isRing</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isSimple</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">isValid</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">isWithinDistance</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">isWithinDistance3D</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">Jenks</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">length</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">lessEqualThan</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">lessThan</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">list</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">log</ogc:Function_Name>
            <ogc:Function_Name nArgs="4">LRSGeocode</ogc:Function_Name>
            <ogc:Function_Name nArgs="-4">LRSMeasure</ogc:Function_Name>
            <ogc:Function_Name nArgs="5">LRSSegment</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">max</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">max_2</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">max_3</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">max_4</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">min</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">min_2</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">min_3</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">min_4</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">mincircle</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">minimumdiameter</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">minrectangle</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">modulo</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">MultiplyCoverages</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">Nearest</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">NetcdfOutput</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">not</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">notEqualTo</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">numberFormat</ogc:Function_Name>
            <ogc:Function_Name nArgs="5">numberFormat2</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">numGeometries</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">numInteriorRing</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">numPoints</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">octagonalenvelope</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">offset</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">overlaps</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">PagedUnique</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">parameter</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">parseBoolean</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">parseDouble</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">parseInt</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">parseLong</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">pi</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">PointBuffers</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">pointN</ogc:Function_Name>
            <ogc:Function_Name nArgs="-6">PointStacker</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">PolygonExtraction</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">polygonize</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">pow</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">property</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">PropertyExists</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">Quantile</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">Query</ogc:Function_Name>
            <ogc:Function_Name nArgs="0">random</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">RangeLookup</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">RasterAsPointCollection</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">RasterZonalStatistics</ogc:Function_Name>
            <ogc:Function_Name nArgs="-6">RasterZonalStatistics2</ogc:Function_Name>
            <ogc:Function_Name nArgs="5">Recode</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">RectangularClip</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">relate</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">relatePattern</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">reproject</ogc:Function_Name>
            <ogc:Function_Name nArgs="-1">ReprojectGeometry</ogc:Function_Name>
            <ogc:Function_Name nArgs="-3">rescaleToPixels</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">rint</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">round</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">round_2</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">roundDouble</ogc:Function_Name>
            <ogc:Function_Name nArgs="-5">ScaleCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">setCRS</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">simplify</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">sin</ogc:Function_Name>
            <ogc:Function_Name nArgs="-2">Snap</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">splitPolygon</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">sqrt</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">StandardDeviation</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">startAngle</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">startPoint</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">StoreCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">strCapitalize</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strConcat</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strEndsWith</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strEqualsIgnoreCase</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strIndexOf</ogc:Function_Name>
            <ogc:Function_Name nArgs="4">stringTemplate</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strLastIndexOf</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">strLength</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strMatches</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">strPosition</ogc:Function_Name>
            <ogc:Function_Name nArgs="4">strReplace</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strStartsWith</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">strSubstring</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">strSubstringStart</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">strToLowerCase</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">strToUpperCase</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">strTrim</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">strTrim2</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">StyleCoverage</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">symDifference</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">tan</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">toDegrees</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">toRadians</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">touches</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">toWKT</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">Transform</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">union</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">UnionFeatureCollection</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">Unique</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">UniqueInterval</ogc:Function_Name>
            <ogc:Function_Name nArgs="-4">VectorToRaster</ogc:Function_Name>
            <ogc:Function_Name nArgs="3">VectorZonalStatistics</ogc:Function_Name>
            <ogc:Function_Name nArgs="1">vertices</ogc:Function_Name>
            <ogc:Function_Name nArgs="2">within</ogc:Function_Name>
          </ogc:Function_Names>
        </ogc:Functions>
      </ogc:Arithmetic_Operators>
    </ogc:Scalar_Capabilities>
  </ogc:Filter_Capabilities>
</WFS_Capabilities>''')

TEST_DESCRIBEFEATURETYPE_RESPONSE = httpretty.Response('''<?xml version="1.0" encoding="UTF-8"?><xsd:schema xmlns:gml="http://www.opengis.net/gml" xmlns:imos="imos.mod" xmlns:xsd="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified" targetNamespace="imos.mod">
  <xsd:import namespace="http://www.opengis.net/gml" schemaLocation="http://geoserver-123.aodn.org.au/geoserver/schemas/gml/2.1.2/feature.xsd"/>
  <xsd:complexType name="anmn_velocity_timeseries_mapType">
    <xsd:complexContent>
      <xsd:extension base="gml:AbstractFeatureType">
        <xsd:sequence>
          <xsd:element maxOccurs="1" minOccurs="0" name="timeseries_id" nillable="true" type="xsd:long"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="file_url" nillable="true" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="size" nillable="true" type="xsd:double"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="site_code" nillable="true" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="platform_code" nillable="true" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="deployment_code" nillable="true" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="geom" nillable="true" type="gml:GeometryPropertyType"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="LATITUDE" nillable="true" type="xsd:double"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="LONGITUDE" nillable="true" type="xsd:double"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="instrument_nominal_depth" nillable="true" type="xsd:float"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="time_coverage_start" nillable="true" type="xsd:dateTime"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="time_coverage_end" nillable="true" type="xsd:dateTime"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="time_deployment_start" nillable="true" type="xsd:dateTime"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="time_deployment_end" nillable="true" type="xsd:dateTime"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="instrument" nillable="true" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="instrument_serial_number" nillable="true" type="xsd:string"/>
        </xsd:sequence>
      </xsd:extension>
    </xsd:complexContent>
  </xsd:complexType>
  <xsd:element name="anmn_velocity_timeseries_map" substitutionGroup="gml:_Feature" type="imos:anmn_velocity_timeseries_mapType"/>
</xsd:schema>
''')

TEST_GETFEATURE_RESPONSE = httpretty.Response('{"type":"FeatureCollection","totalFeatures":780,"features":[{"type":"Feature","id":"anmn_velocity_timeseries_map.fid-6f08f674_166ec2b1090_-67e1","geometry":null,"properties":{"file_url":"IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc"}},{"type":"Feature","id":"anmn_velocity_timeseries_map.fid-6f08f674_166ec2b1090_-67e0","geometry":null,"properties":{"file_url":"IMOS/ANMN/NRS/NRSYON/Velocity/IMOS_ANMN-NRS_AETVZ_20110413T025900Z_NRSYON_FV01_NRSYON-1104-Workhorse-ADCP-27_END-20111014T222900Z_C-20150306T004801Z.nc"}}],"crs":null}')


class TestPipelineWfs(BaseTestCase):
    def test_get_filter_for_file_url(self):
        file_url = 'IMOS/test/file/url'
        xml_filter = get_filter_for_file_url(file_url, property_name='file_url')

        root = etree.fromstring(xml_filter)
        property_name = root.findtext('ogc:PropertyName', namespaces=root.nsmap)
        literal = root.findtext('ogc:Literal', namespaces=root.nsmap)

        self.assertEqual(property_name, 'file_url')
        self.assertEqual(literal, file_url)


class TestWfsBroker(BaseTestCase):
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_GETCAPABILITIES_RESPONSE])
        self.broker = WfsBroker(self.config.pipeline_config['global']['wfs_url'])

    @httpretty.activate
    def test_getfeature_dict(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_GETFEATURE_RESPONSE])
        response = self.broker.getfeature_dict(typename='anmn_velocity_timeseries_map', propertyname='file_url')

        self.assertEqual(len(response['features']), 2)
        self.assertEqual(response['features'][0]['properties']['file_url'],
                         'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc')
        self.assertEqual(response['features'][1]['properties']['file_url'],
                         'IMOS/ANMN/NRS/NRSYON/Velocity/IMOS_ANMN-NRS_AETVZ_20110413T025900Z_NRSYON_FV01_NRSYON-1104-Workhorse-ADCP-27_END-20111014T222900Z_C-20150306T004801Z.nc')

    @httpretty.activate
    def test_get_url_property_name_for_layer(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_DESCRIBEFEATURETYPE_RESPONSE])

        propertyname = self.broker.get_url_property_name_for_layer('anmn_velocity_timeseries_map')
        self.assertEqual('file_url', propertyname)

    @httpretty.activate
    def test_get_url_property_name_for_layer_not_found(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_DESCRIBEFEATURETYPE_RESPONSE])

        # patch the 'valid' candidates
        self.broker.url_propertyname_candidates = ('nonexistent_property', 'another_nonexistent_property')

        with self.assertRaises(RuntimeError):
            _ = self.broker.get_url_property_name_for_layer('anmn_velocity_timeseries_map')

    @httpretty.activate
    def test_query_files_for_layer(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_DESCRIBEFEATURETYPE_RESPONSE, TEST_GETFEATURE_RESPONSE])

        files_for_layer = self.broker.query_urls_for_layer('anmn_velocity_timeseries_map')
        self.assertIsInstance(files_for_layer, IndexedSet)

    @httpretty.activate
    def test_query_file_exists_for_layer_true(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_DESCRIBEFEATURETYPE_RESPONSE, TEST_GETFEATURE_RESPONSE])

        file_to_check = 'IMOS/ANMN/QLD/GBROTE/Velocity/IMOS_ANMN-QLD_AETVZ_20140408T102930Z_GBROTE_FV01_GBROTE-1404-AWAC-13_END-20141022T052930Z_C-20150215T063708Z.nc'

        file_exists = self.broker.query_url_exists_for_layer(layer='anmn_velocity_timeseries_map', name=file_to_check)
        self.assertTrue(file_exists)

    @httpretty.activate
    def test_query_file_exists_for_layer_false(self):
        httpretty.register_uri(httpretty.GET, self.config.pipeline_config['global']['wfs_url'],
                               responses=[TEST_DESCRIBEFEATURETYPE_RESPONSE, TEST_GETFEATURE_RESPONSE])

        file_to_check = "IMOS/ANMN/QLD/GBROTE/Velocity/FILE_THAT_ISNT_IN_RESULTS.nc"

        file_exists = self.broker.query_url_exists_for_layer(layer='anmn_velocity_timeseries_map', name=file_to_check)
        self.assertFalse(file_exists)
