# -*- coding: utf-8 -*-
from qgis.core import QgsProcessing,QgsCoordinateReferenceSystem, QgsProcessingAlgorithm, QgsProcessingMultiStepFeedback, QgsProcessingParameterFile, QgsProcessingParameterFileDestination
import processing
import os

class ValidadoresLADM10(QgsProcessingAlgorithm):
    def name(self):
        return 'etl_modelo_ladm_1_0'

    def displayName(self):
        return 'ETL MODELO LADM COL 1.0'

    def group(self):
        return 'Validadores ETL'

    def groupId(self):
        return 'validadores_etl'

    def createInstance(self):
        return ValidadoresLADM10()

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
        input_gpkg = parameters['input_gpkg']
        output_gpkg = parameters['output_gpkg']
        
        feedback = QgsProcessingMultiStepFeedback(3, feedback)
        results = {}
        outputs = {}
        
        # Define CRS 9377
        crs = QgsCoordinateReferenceSystem('EPSG:9377')
        # Verificación de rutas
        feedback.pushInfo(f"Archivo de entrada: {input_gpkg}")
        feedback.pushInfo(f"Archivo de salida: {output_gpkg}")

        # Crear el directorio de salida si no existe
        directory = os.path.dirname(output_gpkg)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Mapeo de capas según el ejemplo proporcionado
        layer_mapping = {
            'seleccionecoleubaunit': 'col_uebaunit',
            'seleccioneconstruccion': 'lc_construccion',
            'seleccioneconstruccion (2)': 'lc_unidadconstruccion',
            'seleccioneconstruccion (2) (3)': 'extdireccion',
            'seleccionetablapredio': 'lc_predio',
            'seleccionetablapredio (2)': 'lc_derecho',
            'seleccionetablapredio (2) (2)': 'lc_derechotipo',
            'seleccionetablapredio (2) (2) (2)': 'col_unidadadministrativabasicatipo',
            'seleccioneterreno': 'lc_terreno',
            'seleccioneterreno (2)': 'cc_barrio',
            'seleccioneterreno (2) (2)': 'cc_localidadcomuna',
            'seleccioneterreno (2) (2) (2)': 'cc_sectorurbano',
            'seleccioneterreno (2) (2) (2) (2)': 'cc_sectorrural',
            'seleccioneterreno (2) (2) (2) (2) (2)': 'cc_centropoblado',
            'seleccioneterreno (2) (2) (2) (3)': 'cc_corregimiento',
            'seleccioneterreno (2) (2) (3)': 'cc_manzana',
            'seleccioneterreno (2) (3)': 'cc_vereda',
            'seleccioneterreno (2) (3) (2)': 'cc_limitemunicipio',
            'seleccioneterreno (2) (4)': 'cc_perimetrourbano'
        }

        def layer_path(layer_name):
            return f"{input_gpkg}|layername={layer_name}"

        # Modified to include CRS in output for geometric layers
        def create_output_path(table_name, geom=True):
            if geom:
                return f'ogr:dbname=\'{output_gpkg}\' table="{table_name}" (geom) sql='
            return f'ogr:dbname=\'{output_gpkg}\' table="{table_name}"'
        
        # Unir atributos por valor de campo - Construccion
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': [
                'baunit',
                'numero_sotanos',
                'numero_mezanines',
                'avaluo_construccion',
                'numero_semisotanos',
                'numero_pisos', 
                'area_construccion'
            ],
            'FIELD_2': 'ue_lc_construccion',
            'INPUT': layer_path(layer_mapping['seleccioneconstruccion']),
            'INPUT_2': layer_path(layer_mapping['seleccionecoleubaunit']),
            'METHOD': 1,
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable para construccion.")
            return {}

        outputs['UnirAtributosPorValorDeCampo'] = result

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}
        
        

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (4)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Perimetro_Urbano" (geom)',
            'TARGET_CRS': crs

        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Localidad_Comuna" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2) (2) (2) (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Centro_Poblado" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2) (2) (3)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Corregimiento" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2) (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Sector_Urbano" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_unidadconstruccion',
            'INPUT': layer_path(layer_mapping['seleccioneconstruccion (2)']),
            'INPUT_2': layer_path(layer_mapping['seleccionecoleubaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['UnirAtributosPorValorDeCampo'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(7)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (3) (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Limite_Municipio" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(8)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2) (2) (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Sector_Rural" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(9)
        if feedback.isCanceled():
            return {}

        # Crear índice espacial
        alg_params = {
            'INPUT': layer_path(layer_mapping['seleccioneterreno'])
        }
        outputs['CrearNdiceEspacial'] = processing.run('native:createspatialindex', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(10)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (2) (3)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Manzana" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(11)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo
        # Segunda unión para construccion con predio
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': ['numero_predial'],
            'FIELD_2': 't_id',
            'INPUT': result['OUTPUT'],
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio']),
            'METHOD': 1,
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="LC_Construccion" (geom)',
            'PREFIX': '',
            'TARGET_CRS': crs
        }

        outputs['UnirAtributosPorValorDeCampo'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        feedback.setCurrentStep(12)
        if feedback.isCanceled():
            return {}

        # Segunda unión para UnidadConstruccion con predio
        # Primera unión para UnidadConstruccion
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': [
                'baunit',
                'avaluo_unidad_construccion',
                'area_privada_construida',
                'total_pisos',
                'planta_ubicacion',
                'total_locales',
                'total_banios',
                'tipo_planta',
                'lc_construccion',
                'uso',
                'area_construida',
                'total_habitaciones',
                'tipo_unidad_construccion'
            ],
            'FIELD_2': 'ue_lc_unidadconstruccion',
            'INPUT': layer_path(layer_mapping['seleccioneconstruccion (2)']),
            'INPUT_2': layer_path(layer_mapping['seleccionecoleubaunit']),
            'METHOD': 1,
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        unidad_result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        if unidad_result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable para unidad construccion.")
            return {}

        # Segunda unión para UnidadConstruccion con predio
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': ['numero_predial'],
            'FIELD_2': 't_id',
            'INPUT': unidad_result['OUTPUT'],  # Ahora usa el resultado correcto
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio']),
            'METHOD': 1,
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="LC_UnidadDeConstruccion" (geom)',
            'PREFIX': '',
            'TARGET_CRS': crs
        }

        outputs['UnirAtributosPorValorDeCampo_unidad'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        feedback.setCurrentStep(13)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo_dir
        # Unir atributos por valor de campo_dir
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': [
                'baunit',
                'total_habitaciones',
                'uso',
                'total_locales',
                'avaluo_unidad_construccion',
                'tipo_unidad_construccion',
                'total_banios',
                'planta_ubicacion',
                'lc_construccion',
                'total_pisos',
                'area_privada_construida',
                'area_construida',
                'tipo_planta'
            ],
            'FIELD_2': 'ue_lc_unidadconstruccion',
            'INPUT': layer_path(layer_mapping['seleccioneconstruccion (2)']),
            'INPUT_2': layer_path(layer_mapping['seleccionecoleubaunit']),
            'METHOD': 1,
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        result = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        if result is None:
            feedback.pushInfo("Error al ejecutar joinattributestable para unidad construccion.")
            return {}

        outputs['UnirAtributosPorValorDeCampo'] = result
        feedback.setCurrentStep(14)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2) (3)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Vereda" (geom)',
            'TARGET_CRS': crs
            
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(15)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 't_id',
            'FIELDS_TO_COPY': ['baunit'],
            'FIELD_2': 'ue_lc_terreno',
            'INPUT': layer_path(layer_mapping['seleccioneterreno']),
            'INPUT_2': layer_path(layer_mapping['seleccionecoleubaunit']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            
        }
        outputs['UnirAtributosPorValorDeCampo'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(16)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': layer_path(layer_mapping['seleccioneterreno (2)']),
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="CC_Barrio" (geom)',
            'TARGET_CRS': crs
        }
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(17)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo_dir
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'lc_predio_direccion',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 'T_id',
            'INPUT': layer_path(layer_mapping['seleccioneconstruccion (2) (3)']),
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio']),
            'METHOD': 1,
            'PREFIX': '',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        
        outputs['UnirAtributosPorValorDeCampo_dir'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(14)
        if feedback.isCanceled():
            return {}

        # Extraer por expresión para dirección
        alg_params = {
            'EXPRESSION': ' "T_Id" is not NULL',
            'INPUT': outputs['UnirAtributosPorValorDeCampo_dir']['OUTPUT'],
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="Direccion" (geom)',
            'TARGET_CRS': crs
        }
        
        outputs['ExtraerPorExpresin'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        feedback.setCurrentStep(18)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo_terreno
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': [''],
            'FIELD_2': 't_id',
            'INPUT': outputs['UnirAtributosPorValorDeCampo']['OUTPUT'],
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="LC_Terreno" (geom)',
            'PREFIX': '',
            'TARGET_CRS': crs
        }
        outputs['UnirAtributosPorValorDeCampo_terreno'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(19)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo_derecho
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'baunit',
            'FIELDS_TO_COPY': ['tipo'],
            'FIELD_2': 'unidad',
            'INPUT': outputs['UnirAtributosPorValorDeCampo_terreno']['OUTPUT'],
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio (2)']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': 'memory:derecho_temp',
            'PREFIX': '',
            
        }
        outputs['temp_derecho'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(20)
        if feedback.isCanceled():
            return {}
        
        # Crear expresión para ajustar tipo_2 para LC_Derecho
        alg_params = {
            'FIELD_LENGTH': 0,
            'FIELD_NAME': 'tipo',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 1,  # Integer
            'FORMULA': 'tipo',
            'INPUT': outputs['temp_derecho']['OUTPUT'],
            'OUTPUT': 'memory:derecho_ajustado'
        }
        outputs['derecho_ajustado'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback)
        

        # Unir atributos por valor de campo_derecho_tipo
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': outputs['UnirAtributosPorValorDeCampo_terreno']['OUTPUT'],
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio (2) (2) (2)']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="LC_Tipo_predio" (geom)\n',
            'PREFIX': '',
            'TARGET_CRS': crs
        }
        outputs['UnirAtributosPorValorDeCampo_derecho_tipo'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(21)
        if feedback.isCanceled():
            return {}

        # Unir atributos por valor de campo_derecho_tipo
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'tipo',
            'FIELDS_TO_COPY': ['iliCode'],
            'FIELD_2': 'T_id',
            'INPUT': outputs['derecho_ajustado']['OUTPUT'],
            'INPUT_2': layer_path(layer_mapping['seleccionetablapredio (2) (2)']),
            'METHOD': 1,  # Tomar solo los atributos del primer objeto coincidente (uno a uno)
            'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="LC_Derecho" (geom)',
            'PREFIX': '',
            'TARGET_CRS': crs
        }
        outputs['UnirAtributosPorValorDeCampo_derecho_tipo'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        feedback.setCurrentStep(22)
        if feedback.isCanceled():
            return {}
        
        # Consulta SQL para actualizar la columna ilicode
        alg_params = {
            'DATABASE': output_gpkg,
            'SQL': '''
            UPDATE LC_Derecho
            SET ilicode = 
                CASE 
                    WHEN tipo_2 = 1 THEN 'Dominio'
                    WHEN tipo_2 = 2 THEN 'Ocupación'
                    WHEN tipo_2 = 3 THEN 'Posesión'
                END
            WHERE tipo_2 IN (1, 2, 3);
            '''
        }
        processing.run("native:spatialiteexecutesql", alg_params, context=context, feedback=feedback)
        
        # Después del último SQL y antes del return results, añadir:

        # Lista de capas a procesar
        layers_to_fix = [
            'CC_Manzana', 'CC_Vereda', 'CC_Barrio', 'CC_Centro_Poblado', 'CC_Corregimiento',
            'CC_Limite_Municipio', 'CC_Localidad_Comuna', 'CC_Perimetro_Urbano', 'CC_Sector_Rural',
            'CC_Sector_Urbano', 'Direccion', 'LC_Construccion', 'LC_Derecho', 'LC_Terreno',
            'LC_Tipo_predio', 'LC_UnidadDeConstruccion'
        ]

        for layer_name in layers_to_fix:
            input_layer = f"{output_gpkg}|layername={layer_name}"
            
            # Corregir geometrías
            alg_params = {
                'INPUT': input_layer,
                'OUTPUT': 'memory:'
            }
            fixed_geom = processing.run('native:fixgeometries', alg_params, context=context, feedback=feedback)
            
            # Reproyectar a EPSG:9377
            alg_params = {
                'INPUT': fixed_geom['OUTPUT'],
                'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:9377'),
                'OUTPUT': f'ogr:dbname=\'{output_gpkg}\' table="{layer_name}" (geom)'
            }
            processing.run('native:reprojectlayer', alg_params, context=context, feedback=feedback)
            
            feedback.pushInfo(f'Capa {layer_name} procesada: geometrías corregidas y reproyectada a EPSG:9377')

        # Consulta SQL para actualizar SRC
        alg_params = {
            'DATABASE': output_gpkg,
            'SQL': '''
            UPDATE gpkg_geometry_columns 
            SET srs_id = 9377 
            WHERE table_name LIKE 'CC_%' 
            OR table_name LIKE 'LC_%' 
            OR table_name = 'Direccion';
            '''
        }
        processing.run("native:spatialiteexecutesql", alg_params, context=context, feedback=feedback)
        
        return results
    
    def name(self):
        return 'etl_modelo_ladm_1_0'

    def displayName(self):
        return 'ETL MODELO LADM COL 1.0'

    def group(self):
        return 'Validadores ETL'

    def groupId(self):
        return 'validadores_etl'

    def createInstance(self):
        return ValidadoresLADM10()

