# -*- coding: utf-8 -*-
"""
/***************************************************************************
 Validadores
                                 A QGIS plugin
 Este plugin ejecuta validaciones en un GeoPackage
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                             -------------------
        begin                : 2023-10-01
        copyright            : (C) 2023 by Tu Nombre
        email                : tu@email.com
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""

def classFactory(iface):
    """Load Validadores class from file Validadores.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    from .validadores_plugin import ValidadoresPlugin
    return ValidadoresPlugin(iface)