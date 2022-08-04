#!/usr/bin/env python


from mercury import datamap as dmap


def load_transform_map(map_name:str, yaml_file_path:str):

    transformer_builder = dmap.RecordTransformerBuilder(yaml_file_path,
                                                        map_name=map_name)
    return transformer_builder.build()