package com.demograft.ldb4rdbconverter.geometry;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.prep.PreparedPolygon;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import lombok.extern.slf4j.Slf4j;
import org.geotools.geometry.jts.JTSFactoryFinder;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class WKTGeometry implements Serializable {

    private static final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    public static final double PRECISION_MODEL_SCALE = 1e5;
    public static final PrecisionModel precisionModel = new PrecisionModel(PRECISION_MODEL_SCALE);

    private final STRtree areaTree = new STRtree();
    private final String[] areaNames;
    private final Geometry[] geometries;
    private final WKTReader wktReader;

    private transient PreparedPolygon[] preparedAreas;

    public WKTGeometry(File geometryFile, String nameColumn){
        List<String> areaNames = new ArrayList<>();
        List<Geometry> areas = new ArrayList<>();

        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.setHeaderExtractionEnabled(true);
        csvParserSettings.setMaxCharsPerColumn(-1);
        csvParserSettings.getFormat().setDelimiter('|');
        CsvParser parser = new CsvParser(csvParserSettings);

        wktReader = new WKTReader(new GeometryFactory(precisionModel));

        try {
            for(Record record: parser.iterateRecords(geometryFile)){
                String areaName = record.getString(nameColumn);
                String areaWkt = record.getString("wkt");

                areaNames.add(areaName);
                areas.add(wktReader.read(areaWkt));
            }
        }
        catch(ParseException e){
            throw new RuntimeException("Encountered an error parsing records");
        }

        this.areaNames = areaNames.toArray(new String[0]);
        this.geometries = areas.toArray(new Geometry[0]);

        for (int i = 0; i < areas.size(); i++){
            areaTree.insert(geometries[i].getEnvelopeInternal(), i);
        }
        areaTree.build();
    }

    public String getContainingAreaName(double longitude, double latitude) {
        Envelope queryEnvelope = new Envelope(longitude, longitude, latitude, latitude);
        List indices = areaTree.query(queryEnvelope);

        if (indices.isEmpty()) {
            return null;
        }

        if (preparedAreas == null) {
            preparedAreas = new PreparedPolygon[geometries.length];
            for (int i = 0; i < geometries.length; i++) {
                preparedAreas[i] = new PreparedPolygon((Polygonal) geometries[i]);
            }
        }

        Geometry queryGeometry = geometryFactory.toGeometry(queryEnvelope);

        for (Object index : indices) {
            int castedIndex = (int) index;
            if (preparedAreas[castedIndex].contains(queryGeometry)) {
                return areaNames[castedIndex];
            }
        }
        return null;
    }

    public String getContainingAreaName(Coordinate coord) {
        return getContainingAreaName(coord.getLongitude(), coord.getLatitude());
    }
}