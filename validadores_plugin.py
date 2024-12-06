# -*- coding: utf-8 -*-
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox, QMenu, QToolButton
from qgis.core import QgsApplication, QgsProcessingProvider, Qgis, QgsProcessingContext, QgsProcessingFeedback
import os.path
import logging


from .validadores_algorithm import Validadores
from .etl_gpk_1_2 import ValidadoresLADM
from .validadores_dialog import ValidadoresDialog
from .etl_gpk_LADM1_0 import ValidadoresLADM10

class ValidadoresProvider(QgsProcessingProvider):
    def __init__(self):
        super().__init__()

    def loadAlgorithms(self):
        self.addAlgorithm(Validadores())
        self.addAlgorithm(ValidadoresLADM())
        self.addAlgorithm(ValidadoresLADM10())

    def id(self):
        return 'validadoresETL'

    def name(self):
        return self.tr('Validadores ETL')

    def icon(self):
        return QIcon(os.path.join(os.path.dirname(__file__), 'icon_plugin.ico'))

    def createContext(self):
        return QgsProcessingContext()

    def createFeedback(self):
        return QgsProcessingFeedback()

class ValidadoresPlugin:
    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.provider = None
        self.menu = self.tr('&Validadores ETL')
        self.toolbar = self.iface.addToolBar(self.tr('Validadores ETL'))
        self.toolbar.setObjectName('ValidadoresETL')
        
        # Crear el menú desplegable
        self.toolButton = QToolButton()
        self.toolButton.setPopupMode(QToolButton.MenuButtonPopup)
        self.toolButton.setMenu(QMenu())
        self.toolbar.addWidget(self.toolButton)

    def initGui(self):
        # Crear el proveedor de procesamiento
        self.provider = ValidadoresProvider()
        QgsApplication.processingRegistry().addProvider(self.provider)
        
        # Crear acciones para cada herramienta
        self.action_interno = QAction(
            QIcon(os.path.join(self.plugin_dir, 'icon_plugin.ico')),
            'ETL MODELO INTERNO 1.0', 
            self.iface.mainWindow())
        self.action_ladm = QAction(
            QIcon(os.path.join(self.plugin_dir, 'icon_plugin.ico')),
            'ETL MODELO LADM COL 1.2', 
            self.iface.mainWindow())
        
        # Agregar acción para LADM 1.0
        self.action_ladm_10 = QAction(
            QIcon(os.path.join(self.plugin_dir, 'icon_plugin.ico')),
            'ETL MODELO LADM COL 1.0', 
            self.iface.mainWindow())
        
        # Conectar la nueva acción
        self.action_ladm_10.triggered.connect(self.run_ladm_10)
        
        # Agregar al menú desplegable
        self.toolButton.menu().addAction(self.action_interno)
        self.toolButton.menu().addAction(self.action_ladm)
        self.toolButton.menu().addAction(self.action_ladm_10)
        
        
        # Conectar las acciones
        self.action_interno.triggered.connect(self.run_interno)
        self.action_ladm.triggered.connect(self.run_ladm)
        
        # Agregar acciones al menú desplegable
        self.toolButton.menu().addAction(self.action_interno)
        self.toolButton.menu().addAction(self.action_ladm)
        self.toolButton.setDefaultAction(self.action_interno)

    def unload(self):
        QgsApplication.processingRegistry().removeProvider(self.provider)
        self.iface.mainWindow().removeToolBar(self.toolbar)

    def tr(self, message):
        return QCoreApplication.translate('ValidadoresPlugin', message)

    def run_interno(self):
        dialog = ValidadoresDialog()
        dialog.setWindowTitle("ETL MODELO INTERNO 1.0")
        if dialog.exec_():
            self._run_etl(Validadores(), dialog)

    def run_ladm(self):
        dialog = ValidadoresDialog()
        dialog.setWindowTitle("ETL MODELO LADM COL 1.2")
        if dialog.exec_():
            self._run_etl(ValidadoresLADM(), dialog)

    def run_ladm_10(self):
        dialog = ValidadoresDialog()
        dialog.setWindowTitle("ETL MODELO LADM COL 1.0")
        if dialog.exec_():
            self._run_etl(ValidadoresLADM10(), dialog)

    def _run_etl(self, algorithm, dialog):
        input_gpkg = dialog.get_input_gpkg()
        output_gpkg = dialog.get_output_gpkg()

        if not input_gpkg or not output_gpkg:
            self.iface.messageBar().pushMessage(
                "Error", 
                "Por favor, seleccione los archivos de entrada y salida", 
                level=Qgis.Critical
            )
            return

        try:
            # Crear contexto y feedback directamente
            context = QgsProcessingContext()
            feedback = QgsProcessingFeedback()
            
            result = algorithm.processAlgorithm(
                {
                    'input_gpkg': input_gpkg,
                    'output_gpkg': output_gpkg
                }, 
                context=context, 
                feedback=feedback
            )

            if result:
                message = f"Proceso completado con éxito. Archivo guardado en: {output_gpkg}"
                QMessageBox.information(self.iface.mainWindow(), "Éxito", message)
            else:
                message = "El proceso se completó, pero no produjo Todos los resultados. (Normal si es LADM COL 1.0)"
                QMessageBox.warning(self.iface.mainWindow(), "Advertencia", message)
                
        except Exception as e:
            message = f"Error en el proceso: {str(e)}"
            QMessageBox.critical(self.iface.mainWindow(), "Error", message)