package com.demograft.ldb4rdbconverter;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.demograft.ldb4rdbconverter.Helpers.copySelectionToClipboard;

public class GUIController {

    private Stage stage;
    private Parent root;


    @FXML
    Button loadButton;
    @FXML
    Button newButton;

    @FXML
    Label previousFile;
    @FXML
    Button selectPreviousButton;
    @FXML
    Button loadExistingNext;
    @FXML
    Label previousFileError;
    @FXML
    Button selectPreviousBack;
    @FXML
    Label previousData;
    @FXML
    Button selectPreviousDataButton;

    public void initialize() {

    }


    @FXML
    private void loadExistingClicked() throws Exception{
        stage = (Stage) loadButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("loadExisting.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }
    @FXML
    private void createNewClicked() throws Exception{
        stage = (Stage) newButton.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("newFileSelect.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }


    @FXML
    private void selectPreviousDataClicked(){
        FileChooser fileChooser = new FileChooser();
        Stage secondStage = new Stage();
        File inputFile = fileChooser.showOpenDialog(secondStage);
        AppData.setPreviousData(inputFile);
        if(inputFile != null) {
            previousData.setText("File:" + inputFile.getName());
        }
        else{
            AppData.setPreviousData(null);
            previousData.setText("No file selected");
        }
    }

    @FXML
    private void selectPreviousBackClicked() throws Exception{
        stage = (Stage) selectPreviousBack.getScene().getWindow();
        root = FXMLLoader.load(getClass().getClassLoader().getResource("mainScreen.fxml"));
        Scene scene = new Scene(root);
        stage.setScene(scene);
        stage.show();
    }


    @FXML
    private void selectPreviousFileClicked(){
        FileChooser fileChooser = new FileChooser();
        Stage secondStage = new Stage();
        File inputFile = fileChooser.showOpenDialog(secondStage);
        AppData.setPreviousConfig(inputFile);
        if(inputFile != null) {
            previousFile.setText("File:" + inputFile.getName());
        }
        else{
            AppData.setPreviousConfig(null);
            previousFile.setText("No file selected");
        }
    }
    @FXML
    private void previousFileNextClicked() throws Exception {
        if (AppData.getPreviousConfig() == null || AppData.getPreviousData() == null) {
            previousFileError.setVisible(true);
        } else {
            CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setLineSeparator("\n");
            settings.setMaxColumns(1000);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(AppData.getPreviousData());
            String[] headerArray = parser.parseNextRecord().getValues();
            int headerNr = headerArray.length;

            stage = (Stage) loadExistingNext.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("determineMainAttributes.fxml"));
            Scene scene = new Scene(root, 700, 600);
            stage.setScene(scene);
            stage.show();
        }
    }
}