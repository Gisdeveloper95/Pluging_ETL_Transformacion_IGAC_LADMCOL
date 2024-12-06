# -*- coding: utf-8 -*-
from qgis.core import QgsProcessing, QgsProcessingAlgorithm, QgsProcessingMultiStepFeedback, QgsProcessingParameterFile, QgsProcessingParameterFileDestination, QgsProcessingProvider
import processing
import os

class Validadores(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFile('input_gpkg', 'Seleccione el archivo GeoPackage de entrada', extension='gpkg'))
        self.addParameter(QgsProcessingParameterFileDestination('output_gpkg', 'Archivo GeoPackage de salida', 'GeoPackage files (*.gpkg)'))

    def processAlgorithm(self, parameters, context, feedback):
        feedback = QgsProcessingMultiStepFeedback(21, feedback)
        results = {}
        outputs = {}

        input_gpkg = parameters['input_gpkg']
        output_gpkg = parameters['output_gpkg']

        # Verifica la ruta de salida
        print(f"Ruta del archivo de salida: {output_gpkg}")

        # Crear el directorio de salida si no existe
        directory = os.path.dirname(output_gpkg)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Leer automáticamente las capas del GeoPackage
        layers = {
            'col_uebaunit': 'col_uebaunit',
            'lc_unidad': 'cr_unidadconstruccion',
            'lc_caracteristicas': 'ilc_caracteristicasunidadconstruccion',
            'cr_construccionplantatipo': 'cr_construccionplantatipo',
            'cr_unidadconstrucciontipo': 'cr_unidadconstrucciontipo',
            'direccion': 'extdireccion',
            'tabla_predio': 'ilc_predio',
            'tabla_derecho': 'ilc_derecho',
            'tabla_derecho_tipo': 'ilc_derechocatastraltipo',
            'col_unidad_administrativa_basica_tipo': 'col_unidadadministrativabasicatipo',
            'lc_terreno': 'cr_terreno',
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
            'AV_ZHGU': 'vm_zonahomogeneageoeconomicaurbana',
            'AV_ZHFU': 'vm_zonahomogeneafisicaurbana',
            'AV_ZHGR': 'vm_zonahomogeneageoeconomicarural',
            'AV_ZHFR': 'vm_zonahomogeneafisicarural',
        }
        
        

        def layer_path(layer_name):
            return f"{input_gpkg}|layername={layer_name}"

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layers['CC_Limite_Municipio']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Limite_Municipio" (geom)',
        }
        result = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['ExtraerPorExpresin'] = result

        feedback.setCurrentStep(1)
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
        outputs['extractbyexpression'] = result
        
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
            'FIELD_2': 'ue_cr_terreno',
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
            'FIELD': 'T_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_cr_terreno',
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
            'FIELDS_TO_COPY': ['numero_predial_nacional','baunit'],
            'FIELD_2': 'T_id',
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
            'FIELD': 'tipo',
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
            'FIELD': 'ilc_predio_direccion',
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
            'FIELD': 'T_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_cr_unidadconstruccion',
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
            'FIELDS_TO_COPY': ['numero_predial_nacional','baunit'],
            'FIELD_2': 'T_id',
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
        
              
        feedback.setCurrentStep(23)
        if feedback.isCanceled():
            return {}

        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'cr_caracteristicasunidadconstruccion',
            'FIELDS_TO_COPY': ['identificador', 'tipo_unidad_construccion'],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['lc_caracteristicas']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
        feedback.setCurrentStep(24)
        if feedback.isCanceled():
            return {} 
        
    
    
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo_unidad_construccion',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['cr_unidadconstrucciontipo']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        
        
    
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo_planta',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 'T_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layers['cr_construccionplantatipo']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT,
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['JoinAtributos'] = result
        
        
        
        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable.")
            return {}
        

        outputs['JoinAtributos'] = result
        
        feedback.setCurrentStep(22)
        if feedback.isCanceled():
            return {} 



 # Crear el campo adicional 'planta_total'
        alg_params = {
            'FIELD_NAME': 'piso_total',
            'FIELD_TYPE': 2,  # Tipo texto (String)
            'FIELD_LENGTH': 20,  # Longitud del campo ajustada según sea necesario
            'FIELD_PRECISION': 0,
            'NEW_FIELD': True,
            'FORMULA': 'concat(to_string("iliCode_2"), \' \', to_string("planta_ubicacion"))',  # Concatenar los campos con un espacio entre ellos
            'INPUT': result['OUTPUT'],
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="lc_unidadconstruccion" (geom)',
        }

        result = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        outputs['FieldCalculator'] = result

        if result is None:
            feedback.pushInfo("Error al ejecutar fieldcalculator.")
            return {}

        feedback.setCurrentStep(25)
        if feedback.isCanceled():
            return {}


    
    ###
        
        
        
        
        results['Output GeoPackage'] = output_gpkg
        return results
        
        
        
    def name(self):
        return 'etl_modelo_interno'

    def displayName(self):
        return 'ETL MODELO INTERNO 1.0'

    def group(self):
        return 'Validadores ETL'

    def groupId(self):
        return 'validadores_etl'

    def createInstance(self):
        return Validadores()
