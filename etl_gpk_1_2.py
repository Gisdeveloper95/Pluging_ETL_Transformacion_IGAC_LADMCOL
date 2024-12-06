# -*- coding: utf-8 -*-
from qgis.core import QgsProcessing, QgsProcessingAlgorithm, QgsProcessingMultiStepFeedback, QgsProcessingParameterFile, QgsProcessingParameterFileDestination
import processing
import os

class ValidadoresLADM(QgsProcessingAlgorithm):
    def name(self):
        return 'etl_modelo_ladm'

    def displayName(self):
        return 'ETL MODELO LADM COL 1.2'

    def group(self):
        return 'Validadores ETL'

    def groupId(self):
        return 'validadores_etl'

    def createInstance(self):
        return ValidadoresLADM()

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterFile(
                'input_gpkg',
                'Seleccione el archivo GeoPackage de entrada',
                extension='gpkg'
            )
        )
        self.addParameter(
            QgsProcessingParameterFileDestination(
                'output_gpkg',
                'Archivo GeoPackage de salida',
                'GeoPackage files (*.gpkg)'
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        # Obtener los parámetros de entrada y salida
        input_gpkg = parameters['input_gpkg']
        output_gpkg = parameters['output_gpkg']
        
        feedback = QgsProcessingMultiStepFeedback(21, feedback)
        results = {}
        outputs = {}

        # Verifica la ruta de salida
        feedback.pushInfo(f"Ruta del archivo de salida: {output_gpkg}")

        # Crear el directorio de salida si no existe
        directory = os.path.dirname(output_gpkg)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Leer automáticamente las capas del GeoPackage
        layers = {
            'col_uebaunit': 'col_uebaunit',
            'lc_unidad': 'lc_unidadconstruccion',
            'lc_construccion': 'lc_construccion',
            'lc_caracteristicas': 'lc_caracteristicasunidadconstruccion',
            'lc_construccionplantatipo': 'lc_construccionplantatipo',
            'direccion': 'extdireccion',
            'tabla_predio': 'lc_predio',
            'tabla_derecho': 'lc_derecho',
            'tabla_derecho_tipo': 'lc_derechotipo',
            'col_unidad_administrativa_basica_tipo': 'col_unidadadministrativabasicatipo',
            'lc_terreno': 'lc_terreno',
            'CC_Barrio': 'cc_barrio',
            'CC_Localidad_Comuna': 'cc_localidadcomuna',
            'CC_Sector_Urbano': 'cc_sectorurbano',
            'CC_Sector_Rural': 'cc_sectorrural',
            'CC_Centro_Poblado': 'cc_centropoblado',
            'CC_Corregimiento': 'cc_corregimiento',
            'CC_Manzana': 'cc_manzana',
            'CC_Vereda': 'cc_vereda',
            'CC_Limite_Municipio': 'cc_limitemunicipio',
            'CC_Perimetro_Urbano': 'cc_perimetrourbano',
            'AV_ZHGU': 'av_zonahomogeneageoeconomicaurbana',
            'AV_ZHFU': 'av_zonahomogeneafisicaurbana',
            'AV_ZHGR': 'av_zonahomogeneageoeconomicarural',
            'AV_ZHFR': 'av_zonahomogeneafisicarural',
        }
        
        

        def layer_path(layer_name):
            return f"{input_gpkg}|layername={layer_name}"

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Limite_Municipio']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Limite_Municipio" (geom)',
        }
        feedback.pushInfo("Procesando CC_Limite_Municipio...")
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Centro_Poblado']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Centro_Poblado" (geom)',
        }
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['AV_ZHGU']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Zona_homo_geoeconomicaurbana" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['AV_ZHFU']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Zona_homo_fisicaurbana" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        
        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['AV_ZHFR']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Zona_homo_fisicarural" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        
        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['AV_ZHGR']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Zona_homo_geoeconomicarural" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        
        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}
            
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Corregimiento']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Corregimiento" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        
        feedback.setCurrentStep(7)
        if feedback.isCanceled():
            return {}
        
    
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Manzana']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_manzana" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        
        feedback.setCurrentStep(8)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Barrio']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_barrio" (geom)',
        }
        
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin']= result
        
        feedback.setCurrentStep(9)
        if feedback.isCanceled():
            return {}


        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Vereda']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_vereda" (geom)',
        }
        

        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(10)
        if feedback.isCanceled():
            return {}
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Localidad_Comuna']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Localidad_comuna" (geom)',
        }
        

        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(11)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Sector_Urbano']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Sector_urbano" (geom)',
        }
        

        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(12)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Sector_Rural']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Sector_rural" (geom)',
        }
        

        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(13)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Perimetro_Urbano']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Perimetro" (geom)',
        }
        

        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result
        
        feedback.setCurrentStep(14)
        if feedback.isCanceled():
            return {}
        
        
        
        # join-terreno-predio
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_terreno',
            'INPUT': layer_path(layers['lc_terreno']),
            'INPUT_2': layer_path(layers['col_uebaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(15)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 't_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_predio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="lc_predio" (geom)',
        }
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
        feedback.setCurrentStep(16)
        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['col_unidad_administrativa_basica_tipo']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Lc_Tipo_predio" (geom)',
            'PREFIX': ''
        }

        result =  processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        outputs['JoinAtributos'] = result
        
        
        feedback.setCurrentStep(17)
        if feedback.isCanceled():
            return {}
        
        ##derecho

        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_terreno',
            'INPUT': layer_path(layers['lc_terreno']),
            'INPUT_2': layer_path(layers['col_uebaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }
        
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(18)
        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 't_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_predio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
        feedback.setCurrentStep(19)
        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': ['tipo'],
            'FIELD_2': 'unidad',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_derecho']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
        feedback.setCurrentStep(20)
        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo_2',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_derecho_tipo']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="lc_derecho_tipo" (geom)',
            'PREFIX': ''
        }
        
        result =  processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        outputs['JoinAtributos'] = result
        
        
        feedback.setCurrentStep(21)
        if feedback.isCanceled():
            return {}
        
        ##
        
        #construccion
        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_construccion',
            'INPUT': layer_path(layers['lc_construccion']),
            'INPUT_2': layer_path(layers['col_uebaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(18)
        if feedback.isCanceled():
            return {}
        
        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 't_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_predio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="lc_construccion" (geom)',
        }
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
        feedback.setCurrentStep(19)
        if feedback.isCanceled():
            return {}

        #direccion
        
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['direccion']),
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result

        feedback.setCurrentStep(20)
        if feedback.isCanceled():
            return {}

        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'lc_predio_direccion',
            'FIELD_2': 'T_id',
            'FIELDS_TO_COPY': [''],
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_predio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Extdireccion" (geom)',
        }
        
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(21)
        if feedback.isCanceled():
            return {}
        
        
        #unidad
        
        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_unidadconstruccion',
            'INPUT':   layer_path(layers['lc_unidad']),
            'INPUT_2': layer_path(layers['col_uebaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        feedback.setCurrentStep(22)
        if feedback.isCanceled():
            return {}   


        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 't_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['tabla_predio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }
        
        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
              
        feedback.setCurrentStep(21)
        if feedback.isCanceled():
            return {}

        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'lc_caracteristicasunidadconstruccion',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['lc_caracteristicas']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo_planta',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['lc_construccionplantatipo']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
                        
           # Crear el campo adicional 'planta_total'
        alg_params = {
            'FIELD_NAME': 'planta_total',
            'FIELD_TYPE': 2,  # Tipo texto (String)
            'FIELD_LENGTH': 20,  # Longitud del campo ajustada según sea necesario
            'FIELD_PRECISION': 0,
            'NEW_FIELD': True,
            'FORMULA': 'concat(to_string("iliCode"), \' \', to_string("planta_ubicacion"))',  # Concatenar los campos con un espacio entre ellos
            'INPUT': result['OUTPUT'],
             'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="lc_unidadconstruccion" (geom)',
        }
        
        
        result = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['FieldCalculator'] = result

        if result is None:
            feedback.pushInfo("Error al ejecutar fieldcalculator.")
            return {}


        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        else:
            feedback.pushInfo(f"Resultado de la unión cr_terreno: {result['OUTPUT']}")

        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(22)
        if feedback.isCanceled():
            return {} 
        
        
        return {'Output GeoPackage': output_gpkg}
        
        
        
    def name(self):
        return 'etl_modelo_ladm'

    def displayName(self):
        return 'ETL MODELO LADM COL 1.2'

    def group(self):
        return 'Validadores ETL'

    def groupId(self):
        return 'validadores_etl'

    def createInstance(self):
        return ValidadoresLADM()
