package com.demograft.ldb4rdbconverter;

import javafx.beans.property.SimpleStringProperty;


public class DataRow {
    private SimpleStringProperty header;
    private SimpleStringProperty type;
    private SimpleStringProperty examples;

    public DataRow(String header, String type, String examples) {
        this.header = new SimpleStringProperty(header);
        this.type = new SimpleStringProperty(type);
        this.examples = new SimpleStringProperty(examples);
    }

    public String getHeader() {
        return header.get();
    }

    public void setHeader(String header) {
        this.header.set(header);
    }

    public String getType() {
        return type.get();
    }

    public void setType(String type) {
        this.type.set(type);
    }

    public String getExamples() {
        return examples.get();
    }

    public void setExamples(String examples) {
        this.examples.set(examples);
    }
    public SimpleStringProperty headerProperty(){
        return header;
    }
    public SimpleStringProperty typeProperty(){
        return type;
    }
    public SimpleStringProperty examplesProperty(){
        return examples;
    }
}
