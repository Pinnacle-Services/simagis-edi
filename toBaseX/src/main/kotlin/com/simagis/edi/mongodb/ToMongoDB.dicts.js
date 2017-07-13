db.apiJobs.remove({_id: 'dicts'});
db.apiJobs.insertOne({
    "_id" : "dicts",
    "type" : "dicts",
    "status" : "RUNNING",
    "options" : {
        "claimTypes" : {
            "835c" : {
                "target" : "claims_835c"
            }
        }
    }
});
