package com.demograft.ldb4rdbconverter.geometry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GeometryTransformerTest {

    private static final String CELL_IDENTIFIER_FIELD = "cellIdentifier";

    @Test
    public void testNoTransformationIsDoneWhenCellListIsEmpty() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/test/resources/minimalSchema.avsc"));
        GenericData.Record record = createTestRecord(schema);
        List<Cell> cellList = new ArrayList<>();
        GeometryTransformer geometryTransformer = new GeometryTransformer(false, cellList, CELL_IDENTIFIER_FIELD);

        GenericData.Record transformedRecord = geometryTransformer.transformRow(record);

        Assert.assertEquals(record, transformedRecord);
    }

    @Test
    public void testRecordIsTransformedWhenMatchingCellIsFound() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/test/resources/minimalSchema.avsc"));
        GenericData.Record record = createTestRecord(schema);
        float radius = 250.0f;
        List<Cell> cellList = Collections.singletonList(createTestCell(radius));
        GeometryTransformer geometryTransformer = new GeometryTransformer(false, cellList, CELL_IDENTIFIER_FIELD);

        GenericData.Record transformedRecord = geometryTransformer.transformRow(record);

        Assert.assertNotEquals(record, transformedRecord);
        Assert.assertEquals(GeometryType.CIRCLE.name(), transformedRecord.get("geometryType"));
        Assert.assertEquals(0, transformedRecord.get("startAngle"));
        Assert.assertEquals(360, transformedRecord.get("stopAngle"));
        Assert.assertEquals(250, transformedRecord.get("outerSemiMinorRadius"));
        Assert.assertEquals(250, transformedRecord.get("outerSemiMajorRadius"));
    }

    @Test
    public void testRecordWithZeroRadiusCellIsTransformedToPoint() throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/test/resources/minimalSchema.avsc"));
        GenericData.Record record = createTestRecord(schema);
        List<Cell> cellList = Collections.singletonList(createTestCell(0f));
        GeometryTransformer geometryTransformer = new GeometryTransformer(false, cellList, CELL_IDENTIFIER_FIELD);

        GenericData.Record transformedRecord = geometryTransformer.transformRow(record);

        Assert.assertEquals(GeometryType.POINT.name(), transformedRecord.get("geometryType"));
        Assert.assertNull(transformedRecord.get("startAngle"));
        Assert.assertNull(transformedRecord.get("stopAngle"));
        Assert.assertNull(transformedRecord.get("outerSemiMinorRadius"));
        Assert.assertNull(transformedRecord.get("outerSemiMajorRadius"));
    }

    @Test
    public void testAddGeometryJsonObjectsToList() throws IOException, JSONException {
        JSONArray emptyArray = new JSONArray();
        String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/geometryJsonArray.json")), StandardCharsets.UTF_8);

        GeometryTransformer.addGeometryJsonObjectsToList(emptyArray);

        Assert.assertEquals(10, emptyArray.size());
        JSONAssert.assertEquals(emptyArray.toJSONString(), expected, JSONCompareMode.LENIENT);
    }

    private GenericData.Record createTestRecord(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("lat", 58.371208d);
        record.put("lon", 26.7160088d);
        record.put("time", 1591792134L);
        record.put("cellIdentifier", "testCell");
        return record;
    }

    private Cell createTestCell(float radius) {
        Cell testCell = new Cell();
        testCell.setRadius(radius);
        testCell.setCoordinate(new Coordinate(58.371208d, 26.7160088d));
        testCell.setIdentifier("testCell");
        return testCell;
    }
}