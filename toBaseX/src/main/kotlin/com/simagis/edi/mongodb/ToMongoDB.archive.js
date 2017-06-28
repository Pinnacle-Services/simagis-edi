db.apiJobs.remove({_id: 'archive'});
db.apiJobs.insertOne({
    "_id" : "archive",
    "type" : "archive",
    "status" : "RUNNING",
    "options" : {
        "archive" : {
            "835a" : {
                "target" : "claims_835a"
            },
            "837a" : {
                "target" : "claims_837a"
            }
        }
    }
});
