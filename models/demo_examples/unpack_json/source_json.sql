--update my comment

select parse_json('{
    "id": "fekon190-21a8912-dsbna", 
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
