settings = {
    "global_config": {
        "sep": ";",
        "decimal": ".",
        "encoding": "utf8",
        "round_float": 3,
    },
    
    "data_config": {
        "Carga": {
            "endpoints": [
                "/cargaverificada",
                "/cargaprogramada"
            ],
            "days_limit": 31,
            "desirable_columns": [
                'cod_areacarga', 
                'din_atualizacao', 
                'dat_referencia',
                'din_referenciautc',
                "val_cargaglobal",
                "val_cargaglobalprogramada"
            ],
            "col_datetimes": {
                "dat_referencia": r"%Y-%m-%d",
                "dat_referenciautc": r"%Y-%m-%dT%H:%M:%SZ",
                "din_atualizacao": r"%Y-%m-%dT%H:%M:%SZ"
            },
            "df_name": "carga_dessem_semihoraria.csv",
        },
    }
}