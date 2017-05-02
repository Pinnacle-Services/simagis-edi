db.apiJobs.remove({_id: 'debug'});
db.apiJobs.insertOne({
    _id: 'debug',
    type: 'Import',
    status: 'NEW',
    options: {
        "sourceDir": "E:/DATA/source_files",
        "scanMode": "R",
        "xqDir": "isa-claims-xq",
        "parallel": 8,
        "after": "2016-04-28",
        "claimTypes": {
            "835": {
                "name": "DEBUG_claims_835",
                "createIndexes": true
            },
            "837": {
                "name": "DEBUG_claims_837",
                "createIndexes": true
            }
        },
        "archive": {
            "835a": {
                "name": "DEBUG_claims_835a",
                "createIndexes": true
            },
            "837a": {
                "name": "DEBUG_claims_837a",
                "createIndexes": true
            }
        },
        "build835c": {
            "835c": {
                "name": "DEBUG_claims_835c",
                "createIndexes": true
            },
            "clients": "clientid"
        }
    }
});
