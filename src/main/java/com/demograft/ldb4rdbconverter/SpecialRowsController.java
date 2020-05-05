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
import javafx.scene.control.TextField;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpecialRowsController {

    private Stage stage;
    private Parent root;

    @FXML
    TextField timeRows;
    @FXML
    Label timeError;
    @FXML
    TextField IDRows;
    @FXML
    Label uniqueError;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button updateRows;
    @FXML
    Button backButton;
    @FXML
    Button nextButton;
    @FXML
    TextField timeFormat;
    @FXML
    Label formatError;

    public void initialize() {
        updateTable();
    }

    @FXML
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
    private void update(){
        List<String> validHeaders = AppData.getHeaderList();
        Map<String, String> types = AppData.getTypeMap();
        if(!timeRows.getText().equals("")) {
            String[] headers = timeRows.getText().split(",");
            List<String> newTimes = new ArrayList<>();
            for (String header : headers) {
                if (validHeaders.contains(header)) {
                    newTimes.add(header);
                    timeError.setVisible(false);
                    types.put(header, "Long(Time)");
                } else {
                    timeError.setVisible(true);
                    timeError.setText("Invalid header: " + header);
                    break;
                }
            }
            AppData.setTimeRows(newTimes);
        }
        AppData.setTimeExample(timeFormat.getText());
        if(!IDRows.getText().equals("")) {
            String[] uniqueHeaders = IDRows.getText().split(",");
            List<String> newUniques = new ArrayList<>();
            for (String header : uniqueHeaders) {
                if (validHeaders.contains(header)) {
                    newUniques.add(header);
                    uniqueError.setVisible(false);
                    types.put(header, "Long(Hashed)");
                } else {
                    uniqueError.setVisible(true);
                    uniqueError.setText("Invalid header: " + header);
                    break;
                }
            }
            AppData.setHashedRows(newUniques);
        }
        updateTable();
    }
    @FXML
    private void backClicked() throws IOException {
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("rowTypes.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void nextClicked() throws IOException{
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalParameters.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}
