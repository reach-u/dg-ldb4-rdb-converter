package com.demograft.ldb4rdbconverter;


// Here are helper methods. Help to save code by not ctrl+c ctrl+v all methods to each view.

import javafx.collections.ObservableList;
import javafx.scene.control.TablePosition;
import javafx.scene.control.TableView;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;

import java.util.List;

public class Helpers {

    public static void copySelectionToClipboard(final TableView<?> table) {
        ObservableList<TablePosition> posList = table.getSelectionModel().getSelectedCells();
        final StringBuilder strb = new StringBuilder();
        for(TablePosition p: posList){
            int r = p.getRow();
            int c = p.getColumn();
            Object cell = table.getColumns().get(c).getCellData(r);
            if (cell == null)
                cell = "";
            strb.append(cell);
            if(posList.indexOf(p) != posList.size()-1)
            strb.append(",");
        }
        final ClipboardContent clipboardContent = new ClipboardContent();
        clipboardContent.putString(strb.toString());
        Clipboard.getSystemClipboard().setContent(clipboardContent);
    }

    public static String listToString(List<?> list){
        StringBuilder sb = new StringBuilder();
        for(Object object: list){
            if(list.indexOf(object) == list.size() - 1){
                sb.append(object.toString());
            }
            else{
                sb.append(object.toString() + ",");
            }
        }
        return sb.toString();
    }
}
