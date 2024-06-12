# -*- coding: utf-8 -*-
__author__ = 'Miguel Freire Couy'
__credits__ = ['Miguel Freire Couy']
__maintainer__ = 'Miguel Freire Couy'
__email__ = 'miguel.couy@outlook.com'
__status__ = 'Production'

import os
import datetime as dt
from pathlib import Path
from typing import Literal, Optional, Union, List
import pandas as pd
import numpy as np
import requests
import json

from settings import settings

SCRIPT_NAME = os.path.basename(__file__)
SCRIPT_DIR = os.path.dirname(__file__)
SCRIPT_RUN = dt.datetime.now().strftime('%Y-%m-%d %Hh%Mm%Ss')
SCRIPT_CONFIG: dict = settings
GLOBAL_CONFIG: dict = SCRIPT_CONFIG['global_config']


ACCEPT_SUBISTEMAS_AREA = Literal[
    "SECO", "S", "NE", "N",
]

ACCEPT_GEOELETRIC_AREA = Literal[    
    "RJ", "SP", "MG", "ES", "MT", "MS", "DF", "GO", "AC", "RO", "PR",
    "SC", "RS", "BASE", "BAOE", "ALPE", "PBRN", "CE", "PI", "TON", "PA",
    "MA", "AP", "AM", "RR"
]

ACCEPT_LOST_AREA = Literal[
    "PESE", "PES", "PENE", "PEN"
]

ACCEPT_AREAS = Union[
    ACCEPT_SUBISTEMAS_AREA, 
    ACCEPT_GEOELETRIC_AREA, 
    ACCEPT_LOST_AREA
]

subsistemas_list: List[str] = ["SECO", "S", "NE", "N"]
geoelectric_list: List[str] = [
    "RJ", "SP", "MG", "ES", "MT", "MS", "DF", "GO", "AC", "RO", "PR",
    "SC", "RS", "BASE", "BAOE", "ALPE", "PBRN", "CE", "PI", "TON", "PA",
    "MA", "AP", "AM", "RR"
]
lost_area_list: List[str] = ["PESE", "PES", "PENE", "PEN"]



class Carga:
    def __init__(self, config: Optional[dict] = None):
        self.base_url = 'https://apicarga.ons.org.br/prd'
        
        self.set_config(config = config)

    def set_config(self, config: dict) -> None:
        self.config = config

    def set_batches(self,
                     date_from: dt.datetime = None,
                     date_to: Optional[dt.datetime] = None,
                     date_slicer: Optional[int] = None
                     ):
    
        if not date_to:
            date_to = dt.datetime.today()
        if not date_slicer and self.config: 
            date_slicer = self.config["days_limit"]
        if not date_slicer and not self.config:
            date_slicer = 90
        if not date_from:
            return [(
                date_to - dt.timedelta(days = date_slicer),
                date_to
            )]
 
        delta = (date_to - date_from).days
        periods = []

        current_start = date_from
        
        while delta > 0:
            current_end = current_start + dt.timedelta(
                days=min(date_slicer, delta)
            )

            periods.append((current_start, current_end))
            current_start = current_end + dt.timedelta(days=1)
            delta -= date_slicer + 1

        return periods

    def get_data(self,
                 areas: list[ACCEPT_AREAS] = None,
                 date_from: Optional[dt.datetime] = None,
                 date_to: Optional[dt.datetime] = None,
                 config: Optional[dict] = None,
                 not_found_ok: bool = True,
                 ) -> pd.DataFrame:

        if isinstance(areas, str): areas = list(areas)
        if not areas: areas = list(subsistemas_list)
        if config: self.set_config(config = config)  

        final_df = pd.DataFrame()
    
        for batch in self.set_batches(date_from, date_to):
            batch_from, batch_to = batch[0], batch[1]

            for area in areas:
                parameters = {
                    "dat_inicio": batch_from.strftime('%Y-%m-%d'),
                    "dat_fim": batch_to.strftime('%Y-%m-%d'),
                    "cod_areacarga": area
                }
                
                dfs = []
                for endpoint in self.config.get('endpoints'):
                    resp = requests.get(
                        url = self.base_url + endpoint,
                        params = parameters,
                        stream = True
                    )

                    print(resp.status_code, resp.url, sep = '\t')
                    resp.raise_for_status()

                    df = pd.DataFrame(
                        data = json.loads(resp.content)
                    )

                    available_columns = np.intersect1d(
                        df.columns,
                        self.config.get('desirable_columns')
                    )

                    df_filtered = df.loc[:, available_columns]
                    
                    df_filtered['din_referencia'] = pd.to_datetime(
                        df_filtered['din_referenciautc'],
                        utc = True
                    )

                    df_filtered['din_referencia'] = \
                        df_filtered['din_referencia'] - pd.DateOffset(hours = 3)

                    df_filtered.drop(
                        columns = ['din_referenciautc'],
                        inplace = True
                    )
                    
                    if 'din_atualizacao' in df_filtered.columns:
                        df_filtered['din_atualizacao'] = pd.to_datetime(
                            df_filtered['din_atualizacao'],
                            utc = True
                        )

                        df_filtered['din_atualizacao'] = \
                            df_filtered['din_atualizacao'] - pd.DateOffset(
                                hours = 3
                            )

                    dfs.append(df_filtered)

                combined_df = dfs[0]
                for df in dfs[1:]:
                    combined_df = pd.merge(
                        left = combined_df, 
                        right = df, 
                        on = [
                            'cod_areacarga', 
                            'dat_referencia', 
                            'din_referencia'
                        ],
                        how = 'outer'
                    )

                final_df = pd.concat(
                    objs = [final_df, combined_df],
                    ignore_index = True
                )

        final_df.rename(
            columns = {
                "cod_areacarga": "Area Carga",
                "dat_referencia": "Referencia",
                "din_atualizacao": "Atualizacao",
                "din_referencia": "Instante",
                "val_cargaglobal": "Carga Verificada",
                "val_cargaglobalprogramada": "Carga Programada"
            },
            inplace = True
        )

        final_df = final_df[
            ["Area Carga", 'Referencia', 'Instante', 'Atualizacao',
             'Carga Programada', 'Carga Verificada']
        ]

        final_df['Carga Diferenca'] = \
            final_df["Carga Programada"] - final_df["Carga Verificada"] 

        final_df = final_df[final_df['Carga Verificada'] != 0]

        final_df['Atualizacao'] = final_df['Atualizacao'].dt.strftime(
            '%Y-%m-%d %H:%M'
        )
        final_df['Instante'] = final_df['Instante'].dt.strftime(
            '%Y-%m-%d %H:%M'
        )

        return final_df
    
    def save_data(self,
                  dataframe: pd.DataFrame,
                  config: Optional[dict] = None,
                  save_where: Optional[Path] = None
                  ) -> pd.DataFrame:
        """
        Saves the given DataFrame to a specified location.

        Parameters:
        - dataframe (pd.DataFrame): The DataFrame to be saved.
        - config (dict): Configuration dictionary containing key settings 
          for saving the data.
        - save_where (Optional[Path]): Directory to save the file.

        Returns:
        - pd.DataFrame: The saved DataFrame.
        """

        if config: self.set_config(config)
        save_path = save_where if save_where else Path(SCRIPT_DIR, 'data')
        
        os.makedirs(
            name = save_path,
            exist_ok = True
        )

        filepath = Path(save_path, self.config.get('df_name'))

        dataframe.to_csv(
            path_or_buf = filepath,
            index = False,
            sep = GLOBAL_CONFIG['sep'],
            decimal = GLOBAL_CONFIG['decimal'],
            float_format = "%.3f",
            encoding = GLOBAL_CONFIG['encoding']
        )

        return dataframe


if __name__ == '__main__':
    carga_obj = Carga(config = SCRIPT_CONFIG["data_config"]["Carga"])
    carga_df = carga_obj.get_data()
    carga_obj.save_data(carga_df)

