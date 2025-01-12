from pysdmx.api.qb.service import RestService
from pysdmx.api.qb.structure import StructureQuery, StructureFormat, StructureType, StructureReference, StructureDetail
from pysdmx.api.qb.data import DataQuery, DataFormat
from pysdmx.api.qb.util import ApiVersion
from pysdmx.io import read_sdmx, get_datasets

api_endpoint='https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1'

# estat_webservice = RestService(
#     api_endpoint='https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1',
#     api_version= ApiVersion.V1_4_0,
#     structure_format= StructureFormat.SDMX_ML_2_1_STRUCTURE
#     )

def get_metadata_url(resource_id):
    query = StructureQuery(resource_id= resource_id, artefact_type= StructureType.DATAFLOW, references=StructureReference.DESCENDANTS, detail=StructureDetail.FULL)
    return api_endpoint + query.get_url(ApiVersion.V1_4_0)

def get_data_url(resource_id, data_selection=None, start_period=None, end_period=None):
    
    data_selection = data_selection or 'all'
    start_period = f'&startPeriod={start_period}' if start_period else ''
    end_period = f'&endPeriod={end_period}' if end_period else ''


    return f'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{resource_id}/{data_selection}?format=SDMX-CSV{start_period}{end_period}'

def get_eurostat_dataset(resource_id, data_selection=None, start_period=None, end_period=None):
    metadata = read_sdmx(get_metadata_url(resource_id))
    data = read_sdmx(get_data_url(resource_id, data_selection, start_period, end_period))
    
    metadata = metadata.get_dataflows()
    metadata = metadata[list(metadata.keys())[0]]

    dataset = data.get_datasets()[0]

    dataset.structure = metadata
    
    return dataset   

def get_dimensions(metadata):

    dsds = metadata.get_data_structure_definitions()
    dsd = dsds[list(dsds.keys())[0]]

    structure = {}
    structure_num = 1

    for component in dsd.components:
        if str(component.role) == 'Role.DIMENSION':
            structure[structure_num] = component.concept.id
            structure_num += 1
    return structure


def get_summary_metadata(metadata):
    dsds = metadata.get_data_structure_definitions()
    dsd = dsds[list(dsds.keys())[0]]

    dataflows = metadata.get_dataflows()
    dataflow = dataflows[list(dataflows.keys())[0]]

    summary = {}
    summary['dataflow_id'] = dataflow.id
    summary['dataflow_name'] = dataflow.name
    summary['dsd_id'] = dsd.id
    summary['dsd_name'] = dsd.name

    dimension_details = {}

    for component in dsd.components:
        if str(component.role) == 'Role.DIMENSION':
            enumeration = {}
            if component.enumeration is not None:
                for code in component.enumeration.codes:
                    enumeration[code.id] = code.name
            dimension_details[component.concept.id] = {
                'name': component.concept.name,
                'enumeration': enumeration
            }
    summary['dimensions'] = dimension_details
    return summary

def get_summary_dataset(dataset):       
    summary = {}
    summary['structure_type'] = dataset.structure.__class__.__name__
    summary['structure_id'] = dataset.structure.id
    summary['structure_name'] = dataset.structure.name
    
    if summary['structure_type'] == 'Dataflow':
        dsd = dataset.structure.structure
    else:
        dsd = dataset.structure

    dsd_dimensions = {}
    for component in dsd.components:
        if str(component.role) == 'Role.DIMENSION':
            enumeration = {}
            if component.enumeration is not None:
                for code in component.enumeration.codes:
                    enumeration[code.id] = code.name
            dsd_dimensions[component.concept.id] = {
                'name': component.concept.name,
                'enumeration': enumeration
            }

    data_effective_structure_summary = {}

    summary['dsd_dimensions'] = dsd_dimensions

    for component in dataset.data.columns:
        if component in dsd_dimensions.keys():
            data_effective_structure_summary[component] = {
                'code': component,
                'name': dsd_dimensions[component]['name'],
                'enumeration': list(dataset.data[component].unique())
            }
    summary['data_effective_structure_summary'] = data_effective_structure_summary
    return summary


def build_data_selection(selection_dict):
    result = ''
    for key, value in selection_dict.items():
        if isinstance(value, list):
            for item in value:
                result += f'{item}+'
            result = result[:-1]
        else:
            result += value
        result += '.'

    return result[:-1]
