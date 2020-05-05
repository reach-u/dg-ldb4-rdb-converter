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
import java.util.List;

public class FinalParametersController {

    private Stage stage;
    private Parent root;

    @FXML
    TextField stringWarning;
    @FXML
    Label limitError;
    @FXML
    TextField parquetSize;
    @FXML
    Label parquetError;
    @FXML
    Button updateRows;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button backButton;
    @FXML
    Button nextButton;


    public void initialize() {
        updateTable();
    }

    public void update(){
        if(!stringWarning.getText().equals("")){
            try{
                int nr = Integer.parseInt(stringWarning.getText());
                AppData.setStringLimit(nr);
                limitError.setVisible(false);
            }
            catch(NumberFormatException e){
                limitError.setText("Error parsing number");
            }
        }
        if(!parquetSize.getText().equals("")){
            try{
                int nr = Integer.parseInt(parquetSize.getText());
                AppData.setParquetSize(nr);
                parquetError.setVisible(false);
            }
            catch(NumberFormatException e){
                parquetError.setText("Error parsing number");
            }
        }
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
    private void backClicked() throws IOException {
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("specialRows.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void nextClicked() throws IOException{
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("confirmationScreen.fxml"));
        Scene scene = new Scene(root, 1100, 600);
        stage.setScene(scene);
        stage.show();
    }
}
