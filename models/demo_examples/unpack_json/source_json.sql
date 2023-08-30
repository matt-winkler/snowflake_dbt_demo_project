

select parse_json('{
    "id": "fekon190-218912-dsbna", 
    "type": "web", 
    "experiment_ids": [1,2,3],
    "experiment_mappings" : [
            {
                "id": 1,
                "page": "landing"
            },
            {
                "id": 2,
                "page": "checkout"
            }
    ]
}') as data