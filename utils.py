from pysdmx.api.qb.service import RestService
from pysdmx.api.qb.structure import StructureQuery, StructureFormat, StructureType, StructureReference, StructureDetail
from pysdmx.api.qb.data import DataQuery, DataFormat
from pysdmx.api.qb.util import ApiVersion
from pysdmx.io import get_datasets

import pandas as pd


api_endpoint='https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1'


def get_estat_webservice():
    return RestService(
        api_endpoint='https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1',
        api_version= ApiVersion.V1_4_0,
        structure_format= StructureFormat.SDMX_ML_2_1
        )

def get_metadata_url(resource_id):
    query = StructureQuery(resource_id= resource_id, artefact_type= StructureType.DATAFLOW, references=StructureReference.DESCENDANTS, detail=StructureDetail.FULL)
    return api_endpoint + query.get_url(ApiVersion.V1_4_0)

def get_data_url(resource_id, data_selection=None,
                 start_period=None, end_period=None,
                 last_n_obs=None):
    
    data_selection = data_selection or 'all'
    start_period = f'&startPeriod={start_period}' if start_period else ''
    end_period = f'&endPeriod={end_period}' if end_period else ''
    last_n_obs = f'&lastNObservations={last_n_obs}' if last_n_obs else ''

    return f'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{resource_id}/{data_selection}?format=SDMX-CSV{start_period}{end_period}{last_n_obs}'


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

def get_summary_dataset(dataset_id):       

    print(f'data query: {get_data_url(dataset_id, last_n_obs=1)}')
    print(f'metadata query: {get_metadata_url(dataset_id)}')

    dataset = get_datasets(
        data= get_data_url(dataset_id, last_n_obs=1),
        structure=get_metadata_url(dataset_id), validate=False)[0]

    summary = {}
    summary['structure_type'] = dataset.structure.__class__.__name__
    summary['structure_id'] = dataset.structure.id
    
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

    for component in dataset.data.columns:
        if component in dsd_dimensions.keys() and component != 'TIME_PERIOD':
            enumeration = {}
            for code in dataset.data[component].unique():
                enumeration[code] = dsd_dimensions[component]['enumeration'][code]
            data_effective_structure_summary[component] = {
                'code': component,
                'name': dsd_dimensions[component]['name'],
                'enumeration': enumeration
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


def get_labels_df(dataset, dim):
    result = []
    if dataset.structure.__class__.__name__ == 'Dataflow':
        dsd = dataset.structure.structure
    else:
        dsd = dataset.structure
    for component in dsd.components:
        if component.id == dim:
            if component.enumeration is not None:
                for code in component.enumeration.codes:
                    result.append({
                        'id': code.id,
                        'name': code.name})

            break 
    return pd.DataFrame(result)


def add_labels(dataset, dim):
    labels = get_labels_df(dataset, dim)
    dataset.data = dataset.data.\
        merge(labels, left_on=dim, right_on='id').\
        drop(columns=['id']).\
            rename(columns={'name': f'{dim}_label'})
    return dataset


def get_dataset_with_selection(dataset_id, selection_dict):

    data_selection = build_data_selection(selection_dict)
    data_url = get_data_url(dataset_id, data_selection=data_selection, start_period=2000)
    metadata_url = get_metadata_url(dataset_id)

    dataset = get_datasets(data_url, metadata_url, validate=False)[0]
    dataset.data.TIME_PERIOD = dataset.data.TIME_PERIOD.astype(int)
    dataset.data.OBS_VALUE = dataset.data.OBS_VALUE.astype(float)

    return dataset

def compare_aggregates(dataset_id, constraints_1, constraints_2, group_by='TIME_PERIOD', threshold=5):
    ds1 = get_dataset_with_selection(dataset_id, constraints_1)
    ds2 = get_dataset_with_selection(dataset_id, constraints_2)

    ds1_agg = ds1.data.groupby(group_by)['OBS_VALUE'].sum().reset_index()
    ds2_agg = ds2.data.groupby(group_by)['OBS_VALUE'].sum().reset_index()

    merged = ds1_agg.merge(ds2_agg, on=group_by, suffixes=('_1', '_2'))
    merged['check'] = abs(merged['OBS_VALUE_1'] - merged['OBS_VALUE_2']) < threshold
    merged = merged[~merged['check']]
    merged['imbalance'] = abs(merged['OBS_VALUE_1'] - merged['OBS_VALUE_2'])
    merged['imbalance_ratio'] = round(merged['imbalance'] / merged['OBS_VALUE_1'], 4)

    return merged

#Add methods to check density and to check whether a total = sum(subtotals)