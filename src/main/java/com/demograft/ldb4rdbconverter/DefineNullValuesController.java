package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.demograft.ldb4rdbconverter.Helpers.copySelectionToClipboard;

public class DefineNullValuesController {

    private Stage stage;
    private Parent root;

    private List<String> listStringNulls;
    private List<Double> listDoubleNulls;
    private List<Long> listLongNulls;
    private List<Float> listFloatNulls;

    @FXML
    Button updateNullsButton;
    @FXML
    TextField longNulls;
    @FXML
    Label longError;
    @FXML
    TextField floatNulls;
    @FXML
    Label floatError;
    @FXML
    TextField doubleNulls;
    @FXML
    Label doubleError;
    @FXML
    TextField stringNulls;
    @FXML
    Label stringError;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button defineNullsBack;
    @FXML
    Button defineNullsNext;



    public void initialize() {
        listDoubleNulls = new ArrayList<>(AppData.getDoubleNulls());
        listStringNulls = new ArrayList<>(AppData.getStringNulls());
        listFloatNulls = new ArrayList<>(AppData.getFloatNulls());
        listLongNulls = new ArrayList<>(AppData.getLongNulls());
        StringBuilder sb = new StringBuilder();
        for(Double number: listDoubleNulls){
            sb.append(number.toString() + ",");
        }
        doubleNulls.setText(sb.toString());
        sb = new StringBuilder();
        for(Float number: listFloatNulls){
            sb.append(number.toString() + ",");
        }
        floatNulls.setText(sb.toString());
        sb = new StringBuilder();
        for(Long number: listLongNulls){
            sb.append(number.toString() + ",");
        }
        longNulls.setText(sb.toString());
        sb = new StringBuilder();
        for(String string: listStringNulls){
            sb.append(string + ",");
        }
        stringNulls.setText(sb.toString());
        mainTable.getSelectionModel().setCellSelectionEnabled(true);
        mainTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        // Implements ctrl + c copying from tableview. Allows selection of multiple rows.

        final KeyCodeCombination keyCodeCopy = new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_ANY);
        mainTable.setOnKeyPressed(event -> {
            if (keyCodeCopy.match(event)) {
                copySelectionToClipboard(mainTable);
            }
        });
        updateTable();
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
    private void updateNulls(){
        if(!longNulls.getText().equals("")){
            List<Long> nullValues = new ArrayList<>();
            String all = longNulls.getText();
            String[] values = all.split(",");
            boolean faulty = false;
            for(String nullValue: values){
                try{
                    Long longNr = Long.parseLong(nullValue);
                    nullValues.add(longNr);
                }
                catch(NumberFormatException e){
                    longError.setVisible(true);
                    longError.setText("Couldn't convert all values.");
                    faulty = true;
                }
            }
            if(!faulty) {
                listLongNulls = new ArrayList<>(nullValues);
                AppData.setLongNulls(new ArrayList<>(nullValues));
                longError.setVisible(false);
            }
        }
        if(!floatNulls.getText().equals("")){
            List<Float> nullValues = new ArrayList<>();
            String all = floatNulls.getText();
            String[] values = all.split(",");
            boolean faulty = false;
            for(String nullValue: values){
                try{
                    Float longNr = Float.parseFloat(nullValue);
                    nullValues.add(longNr);
                }
                catch(NumberFormatException e){
                    floatError.setVisible(true);
                    floatError.setText("Couldn't convert all values.");
                    faulty = true;
                }
            }
            if(!faulty) {
                listFloatNulls = new ArrayList<>(nullValues);
                AppData.setFloatNulls(new ArrayList<>(nullValues));
                floatError.setVisible(false);
            }
        }
        if(!doubleNulls.getText().equals("")){
            List<Double> nullValues = new ArrayList<>();
            String all = doubleNulls.getText();
            String[] values = all.split(",");
            boolean faulty = false;
            for(String nullValue: values){
                try{
                    Double doubleNr = Double.parseDouble(nullValue);
                    nullValues.add(doubleNr);
                }
                catch(NumberFormatException e){
                    doubleError.setVisible(true);
                    doubleError.setText("Couldn't convert all values.");
                    faulty = true;
                }
            }
            if(!faulty) {
                listDoubleNulls = new ArrayList<>(nullValues);
                AppData.setDoubleNulls(new ArrayList<>(nullValues));
                doubleError.setVisible(false);
            }
        }
        if(!stringNulls.getText().equals("!")){
            List<String> nullValues = new ArrayList<>();
            String all = stringNulls.getText();
            String[] values = all.split(",");
            for(String nullValue: values){
                nullValues.add(nullValue);
            }
            listStringNulls = new ArrayList<>(nullValues);
            AppData.setStringNulls(new ArrayList<>(nullValues));
        }
        else{
            listStringNulls = new ArrayList<>();
            AppData.setStringNulls(new ArrayList<>());
            stringError.setVisible(false);
        }
    }

    @FXML
    private void nullsBack() throws IOException{
        stage = (Stage) defineNullsBack.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("removeColumns.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void nullsNext() throws IOException {
        stage = (Stage) defineNullsNext.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("rowTypes.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}
