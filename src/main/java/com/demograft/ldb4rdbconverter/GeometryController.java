package com.demograft.ldb4rdbconverter;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
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
import java.util.Map;

import static com.demograft.ldb4rdbconverter.Helpers.copySelectionToClipboard;

public class GeometryController {

    private Stage stage;
    private Parent root;

    @FXML
    TextField cellLocationId;
    @FXML
    TextField radiusField;
    @FXML
    CheckBox checkBox;
    @FXML
    Button updateGeometry;
    @FXML
    TableView<DataRow> mainTable;
    @FXML
    Button backButton;
    @FXML
    Button nextButton;
    @FXML
    CheckBox uncertainty;
    @FXML
    Label geometryError;
    @FXML
    TextField searchBar;
    @FXML
    private FilteredList<DataRow> filteredList;
    private ObservableList<DataRow> dataList;


    public void initialize() {
        mainTable.getSelectionModel().setCellSelectionEnabled(true);
        mainTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        // Implements ctrl + c copying from tableview. Allows selection of multiple rows.

        final KeyCodeCombination keyCodeCopy = new KeyCodeCombination(KeyCode.C, KeyCombination.CONTROL_ANY);
        mainTable.setOnKeyPressed(event -> {
            if (keyCodeCopy.match(event)) {
                copySelectionToClipboard(mainTable);
            }
        });

        // Add a listener to only make the geometry fields editable when the checkbox is selected

        checkBox.selectedProperty().addListener(new ChangeListener<Boolean>() {
            @Override
            public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
                if(newValue){
                    cellLocationId.setDisable(false);
                    radiusField.setDisable(false);
                    uncertainty.setDisable(false);
                    updateGeometry.setDisable(false);
                }else{
                    cellLocationId.setDisable(true);
                    radiusField.setDisable(true);
                    uncertainty.setDisable(true);
                    updateGeometry.setDisable(true);
                }
            }
        });
        dataList = FXCollections.observableArrayList(AppData.getGUIexamples());
        filteredList = new FilteredList<>(dataList, p -> true);
        searchBar.setOnKeyReleased(keyEvent -> {
            filteredList.setPredicate(p -> p.getHeader().toLowerCase().contains(searchBar.getText().toLowerCase().trim()));
        });
        mainTable.setItems(filteredList);
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
        dataList.clear();
        dataList.addAll(newList);
    }

    @FXML
    private void update(){
        Map<String, String> types = AppData.getTypeMap();
        if(cellLocationId.getText().equals("") && radiusField.getText().equals("")){
            geometryError.setText("One field is required");
            geometryError.setVisible(true);
        }
        if(!cellLocationId.getText().equals("") && AppData.getHeaderList().contains(cellLocationId.getText())){
            types.put(cellLocationId.getText(), "CellId");
            geometryError.setVisible(false);
            AppData.setCellId(cellLocationId.getText());
            if(uncertainty.isSelected()){
                AppData.setUncertainty("true");
            }
            else{
                AppData.setUncertainty("false");
            }
        }
        if(!radiusField.getText().equals("") && AppData.getHeaderList().contains(radiusField.getText())){
            types.put(radiusField.getText(), "Radius");
            geometryError.setVisible(false);
            AppData.setRadiusField(radiusField.getText());
            if(uncertainty.isSelected()){
                AppData.setUncertainty("true");
            }
            else{
                AppData.setUncertainty("false");
            }

        }
        updateTable();
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
        root = FXMLLoader.load(getClass().getClassLoader().getResource("finalParameters.fxml"));
        Scene scene = new Scene(root, 700, 600);
        stage.setScene(scene);
        stage.show();
    }


}
