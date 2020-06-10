package com.demograft.ldb4rdbconverter;

import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;

@Data
public class FieldConversionResult {
    private GenericRecordBuilder genericRecordBuilder;
    private boolean isFaulty;

    public FieldConversionResult(Schema schema) {
        this.genericRecordBuilder = new GenericRecordBuilder(schema);
        this.isFaulty = false;
    }

    public void updateGenericRecordBuilder (Schema.Field field, Object object) {
        this.genericRecordBuilder = genericRecordBuilder.set(field, object);
    }

    public void updateGenericRecordBuilder (String field, Object object) {
        this.genericRecordBuilder = genericRecordBuilder.set(field, object);
    }
}
