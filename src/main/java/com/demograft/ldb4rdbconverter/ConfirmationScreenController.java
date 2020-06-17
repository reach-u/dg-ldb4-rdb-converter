package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;
import javafx.stage.Stage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfirmationScreenController {
    private Stage stage;
    private Parent root;

    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button backButton;
    @FXML
    Button nextButton;
    @FXML
    Label latitudeLabel;
    @FXML
    Label longitudeLabel;
    @FXML
    Label timeLabel;
    @FXML
    Label timezoneLabel;
    @FXML
    Label excludedLabel;
    @FXML
    Label uniquesLabel;
    @FXML
    Label hashedLabel;
    @FXML
    Label doubleNullsLabel;
    @FXML
    Label floatNullsLabel;
    @FXML
    Label stringNullsLabel;
    @FXML
    Label stringColumnsLabel;
    @FXML
    Label floatColumnsLabel;
    @FXML
    Label doubleColumnsLabel;
    @FXML
    Label timeColumnsLabel;
    @FXML
    Label geoFunctionLabel;
    @FXML
    Label uncertaintyLabel;



    public void initialize(){
        latitudeLabel.setText(AppData.getLatitude());
        longitudeLabel.setText(AppData.getLongitude());
        timeLabel.setText(AppData.getTime());
        timezoneLabel.setText(AppData.getTimeExample());
        excludedLabel.setText(Helpers.listToString(AppData.getRemoved()));
        uniquesLabel.setText(String.valueOf(AppData.getStringLimit()));
        hashedLabel.setText(Helpers.listToString(AppData.getHashedRows()));
        doubleNullsLabel.setText(Helpers.listToString(AppData.getDoubleNulls()));
        floatNullsLabel.setText(Helpers.listToString(AppData.getFloatNulls()));
        stringNullsLabel.setText(Helpers.listToString(AppData.getStringNulls()));
        stringColumnsLabel.setText(Helpers.listToString(AppData.getStringColumns()));
        floatColumnsLabel.setText(Helpers.listToString(AppData.getFloatColumns()));
        doubleColumnsLabel.setText(Helpers.listToString(AppData.getDoubleColumns()));
        timeColumnsLabel.setText(Helpers.listToString(AppData.getTimeRows()));
        if(AppData.getUncertainty().equals("false")){
            uncertaintyLabel.setText("false");
        }
        else{
            uncertaintyLabel.setText("true");
        }
        if(AppData.getRadiusField().equals("") && AppData.getCellId().equals("")){
            geoFunctionLabel.setText("none");
        }
        else if(!AppData.getRadiusField().equals("")){
            geoFunctionLabel.setText(AppData.getRadiusField());
        }
        else{
            geoFunctionLabel.setText(AppData.getCellId());
        }
        updateTable();
    }

    private void writeConfigFile(){
        File output;
        if(AppData.getConfigName().equals("")){
            output = new File("ParqConf.properties");
        }
        else{
            output = new File(AppData.getConfigName());
        }
        try(FileWriter fw = new FileWriter(output)){
            fw.write("input-file=" + AppData.getInputFile().getName() + "\n");
            fw.write("output-file=" + output.getName() + "\n");
            fw.write("stats-file=" + output.getName()+ "_stats\n");
            if(AppData.getParquetSize() != 0){
                fw.write("parquet-size=" + AppData.getParquetSize() + "\n");
            }
            // Headers property still needs to be configured.

            fw.write("latitude=" + AppData.getLatitude() + "\n");
            fw.write("longitude=" + AppData.getLongitude() + "\n");
            fw.write("time=" + AppData.getTime() + "\n");
            if(!AppData.getTimeExample().equals("")) {
                fw.write("timezone=" + AppData.getTimeExample() + "\n");
            }
            if(AppData.getRemoved().size() != 0){
                fw.write("excluded=");
                for(String row: AppData.getRemoved()){
                    if(AppData.getRemoved().indexOf(row) == (AppData.getRemoved().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getStringLimit() != 0){
                fw.write("unique-strings=" + AppData.getStringLimit() + "\n");
            }
            if(AppData.getHashedRows().size() != 0){
                fw.write("columns-to-map-long=");
                for(String row: AppData.getHashedRows()){
                    if(AppData.getHashedRows().indexOf(row) == (AppData.getHashedRows().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getLongNulls().size() != 0){
                fw.write("long-null-values=");
                for(Long row: AppData.getLongNulls()){
                    if(AppData.getLongNulls().indexOf(row) == (AppData.getLongNulls().size() - 1)){
                        fw.write(String.valueOf(row));
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getDoubleNulls().size() != 0){
                fw.write("double-null-values=");
                for(Double row: AppData.getDoubleNulls()){
                    if(AppData.getDoubleNulls().indexOf(row) == (AppData.getDoubleNulls().size() - 1)){
                        fw.write(String.valueOf(row));
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getFloatNulls().size() != 0){
                fw.write("float-null-values=");
                for(Float row: AppData.getFloatNulls()){
                    if(AppData.getFloatNulls().indexOf(row) == (AppData.getFloatNulls().size() - 1)){
                        fw.write(String.valueOf(row));
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getStringNulls().size() != 0){
                fw.write("string-null-values=");
                for(String row: AppData.getStringNulls()){
                    if(AppData.getStringNulls().indexOf(row) == (AppData.getStringNulls().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getFloatColumns().size() != 0){
                fw.write("float-columns=");
                for(String row: AppData.getFloatColumns()){
                    if(AppData.getFloatColumns().indexOf(row) == (AppData.getFloatColumns().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getDoubleColumns().size() != 0){
                fw.write("double-columns=");
                for(String row: AppData.getDoubleColumns()){
                    if(AppData.getDoubleColumns().indexOf(row) == (AppData.getDoubleColumns().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getStringColumns().size() != 0){
                fw.write("string-columns=");
                for(String row: AppData.getStringColumns()){
                    if(AppData.getStringColumns().indexOf(row) == (AppData.getStringColumns().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(AppData.getTimeRows().size() != 0){
                fw.write("time-columns=");
                for(String row: AppData.getTimeRows()){
                    if(AppData.getTimeRows().indexOf(row) == (AppData.getTimeRows().size() - 1)){
                        fw.write(row);
                    }
                    else{
                        fw.write(row + ",");
                    }
                }
                fw.write("\n");
            }
            if(!AppData.getRadiusField().equals("")){
                fw.write("radius=" + AppData.getRadiusField() + "\n");
            }
            if(!AppData.getCellId().equals("")){
                fw.write("cell-location-identifier=" + AppData.getCellId() + "\n");
            }
            fw.write("is-coordinate-randomized-in-uncertainty=" + AppData.getUncertainty());
            fw.write("default-type=" + AppData.getDefaultType());
        }
        catch(IOException e){
            System.out.println("Unknown error while writing configuration file");
        }
    }

    private void updateTable(){
        List<DataRow> newList = new ArrayList<>();
        for(int i = 0; i < AppData.getHeaderList().size(); i++){
            List<String> headers = AppData.getHeaderList();
            StringBuilder sb = new StringBuilder();
            for(int j = 0; j < 3; j++){
                sb.append(AppData.getExamples().get(j).get(i) + "; ");
            }
            DataRow row = new DataRow(headers.get(i),AppData.getTypeMap().get(headers.get(i)), sb.toString());
            newList.add(row);
        }
        AppData.setGUIexamples(newList);
        ObservableList<DataRow> olist = FXCollections.observableArrayList(newList);
        mainTable.setItems(olist);
    }

    @FXML
    private void backClicked() throws IOException {
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalParameters.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }

    @FXML
    private void nextClicked() throws IOException{
        writeConfigFile();
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalScreen.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}
