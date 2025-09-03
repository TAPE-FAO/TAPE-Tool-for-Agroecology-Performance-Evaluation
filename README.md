# TAPE-Tool-for-Agroecology-Performance-Evaluation

Standardized framework and tools to assess agroecological performance (TAPE) at farm, project, and national levels.

## TAPE-Tool-for-Agroecology-Performance-Evaluation

Welcome to the GitHub repository for the TAPE (Tool for Agroecology Performance Evaluation) tool. This tool, developed by the Food and Agriculture Organization (FAO), is designed to assess transitions towards sustainable agricultural and food systems. We therefore expect that you have downloaded the data from the KoboToolbox as an Excel-file with XLM values and headers.

The TAPE tool is a powerful instrument that allows users to calculate various parameters related to agroecology performance. It consists of three main components:

Main Script: This is where you can select the parameters you wish to calculate. It serves as the primary interface for interacting with the tool.

Functions File: This file contains the calculations for each parameter. It's the engine that powers the tool, performing all the necessary computations based on your selections in the Main script. This file is found in the folder "Sourced code"

config.yml: This is where you can enter the location of the file(s) your draw your data from, as well as set diffferent configurations for the main script.

Visualise.R: Here you find some plot options commonly used with the calculated TAPE data.

This repository contains all the code necessary to run these scripts and use the TAPE tool. Whether you're a researcher studying sustainable agriculture, a policy maker looking for data to inform decisions, or simply someone interested in agroecology, this tool can provide valuable insights.

TAPE is constantly evolving. The version of the tool in this script is of the year 2023 and has slight changes compared to older versions (different variable names, new data source file). The older version of the script (2022 version) can be found under "Releases". The methodology for the new biodiversity index is described in the paper: How to assess the agroecological status of Swiss farming systems?, Gilgen et al., 2023 (<https://doi.org/10.34776/as172e>)

Stay tuned for more updates and improvements to the TAPE tool as we continue our mission of promoting sustainable agriculture and food systems.

How to use this repository config.yml First you open the config.yml file. There you enter the path to where you have saved the excel file. Other configurations can be met here too, like changes to the Livestock-Unit and the choice of a different versions of the Biodiversity Indicator ("Agrobiodiversity" for the standard TAPE agrobiodiversity index or "Biodiversity" for the improved biodiversity index described by Gilgen et al. (2023)). To apply the changes made you have to save the config.yml file. Right now there are different pre-defined international Regions with their respective Livestock-units (taken from: Guidelines for the preparation of livestock sector reviews - FAO, Annex 1, <https://www.fao.org/3/i2294e/i2294e00.pdf>). The livestock units can be changed manually as well.
