# -*- coding: utf-8 -*-
import os
from qgis.PyQt import uic
from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import QFileInfo

FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'validadores_dialog_base.ui'))

class ValidadoresDialog(QtWidgets.QDialog, FORM_CLASS):
    def __init__(self, parent=None):
        """Constructor."""
        super(ValidadoresDialog, self).__init__(parent)
        self.setupUi(self)
        self.input_gpkg.fileChanged.connect(self.update_output_path)

    def update_output_path(self):
        input_path = self.input_gpkg.filePath()
        if input_path:
            file_info = QFileInfo(input_path)
            dir_path = file_info.absolutePath()
            base_name = file_info.baseName()
            suggested_output = os.path.join(dir_path, f"{base_name}_output.gpkg")
            self.output_gpkg.setFilePath(suggested_output)

    def get_input_gpkg(self):
        return self.input_gpkg.filePath()

    def get_output_gpkg(self):
        return self.output_gpkg.filePath()
        return self.output_gpkg.filePath()