package com.demograft.ldb4rdbconverter;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TableView;
import javafx.stage.Stage;

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


    public void initialize(){
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
    private void backClicked() throws IOException {
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalParameters.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }

    @FXML
    private void nextClicked() throws IOException{
        stage = (Stage) backButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalScreen.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }
}
