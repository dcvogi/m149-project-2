const { request } = require("express");

const db_queries = [
{
    query: async function(db, queryParams){
        let startDate = queryParams.startDate;
        let endDate = queryParams.endDate;
        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    creation_date: {
                        $gte: new Date(startDate.toString()),
                        $lt: new Date(endDate.toString())
                    }
                }
            },{
                $group: {
                    "_id": "$service_request_type",
                    "numberOfDocuments": { "$sum": 1}
                }
            },{
                $sort: {
                    "numberOfDocuments": -1
                }
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }    
    }
},{
    query: async function(db, queryParams){
        let startDate = queryParams.startDate;
        let endDate = queryParams.endDate;
        let requestType = queryParams.requestType;
        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    $and: [{
                        creation_date: {
                            $gte: new Date(startDate.toString()),
                            $lt: new Date(endDate.toString())
                        },
                        service_request_type: requestType
                    }]
                }
            },{
                $group: {
                    "_id": "$creation_date",
                    "numberOfDocuments": { "$sum": 1}
                }
            },
            {
                $sort: {
                    "_id": 1
                }
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }    
    }
},{
    query: async function(db, queryParams){
        var dt = queryParams.date;
        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    $and: [{
                        "creation_date": new Date(dt),
                        "zip_code": { $ne: null}
                    }]
                }
            },{
                $group: {
                    "_id": {
                        "zip_code": "$zip_code",
                        "service_request_type": "$service_request_type"
                    },
                    "numberOfRequests": { "$sum": 1}
                }
            },
            {
                $sort: {
                    "_id.zip_code": -1,
                    "numberOfRequests": -1
                }
            },{
                $group: {
                    "_id": "$_id.zip_code",
                    "request_types": {
                        $push: {
                            "type": "$_id.service_request_type",
                            "requests": "$numberOfRequests"
                        }
                    }
                }
            },{
                $project: {
                    "request_types": {
                        $slice: ["$request_types", 3]
                    }
                }
            },
            {
                $sort: {
                    "_id": -1
                }
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }    
    }
},{
    query: async function(db, queryParams){
        var requestType = queryParams.requestType;

        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    $and: [{
                        "service_request_type": requestType,
                        "ward": { $ne: null }
                    }]
                }
            },{
                $group: {
                    "_id": "$ward",
                    "numberOfRequests": { "$sum": 1}
                }
            },
            {
                $sort: {
                    "numberOfRequests": 1
                }
            },{
                $limit: 3
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }    
    }
},{
    query: async function(db, queryParams){
        let startDate = queryParams.startDate;
        let endDate = queryParams.endDate;        
        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    $and: [{
                        creation_date: {
                            $gte: new Date(startDate.toString()),
                            $lt: new Date(endDate.toString())
                        },
                        completion_date: { $ne: null }
                    }]
                    
                }
            },{ 
                "$project": {
                    "service_request_type": "$service_request_type",
                    "completion_time_in_hours": {
                        "$divide": [
                            { "$subtract": ["$completion_date", "$creation_date"] },
                            60 * 1000 * 60
                        ]
                    }
                }
            },{
                $group: {
                    "_id": "$service_request_type",
                    "avg_completion_time_in_hours": {
                        $avg: "$completion_time_in_hours" 
                    }
                }
            },{
                $sort: {
                    "avg_completion_time_in_hours": 1
                }
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }  
    }
},{
    query: async function(db, queryParams){
        let dt = queryParams.date;
        let bottomLeftX = Number(queryParams.botX);
        let bottomLeftY = Number(queryParams.botY);
        let upperRightX = Number(queryParams.upperX);
        let upperRightY = Number(queryParams.upperY); 

        let cursor = await db.collection('requests').aggregate([
            {
                $match: {
                    $and: [{
                        "creation_date": new Date(dt),
                        "longitude": { $ne: null },
                        "latitude": { $ne: null },
                        "location": {
                            "$geoWithin": {
                               "$box": [
                                  [bottomLeftX, bottomLeftY],
                                  [upperRightX, upperRightY]
                               ]
                            }
                        } 
                    }]
                    
                }
            },{
                $group: {
                    "_id": "$service_request_type",
                    "count": { $sum: 1}
                }
            },{
                $sort: {
                    "count": -1
                }
            },{
                $limit: 1
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }  
    }
},{
    query: async function(db, queryParams){
        let dt = queryParams.date;

        let cursor = await db.collection('upvotes').aggregate([
            {
                $match: {
                    "creation_date": new Date(dt)
                }
            },{
                $group: {
                    "_id": {
                        "requestId": "$requestId",
                        "request_number": "$service_request_number"
                    },
                    "count": { $sum: 1}
                }
            },{
                $sort: {
                    "count": -1
                }
            },{
                $limit: 50
            }
        ])

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }  
    }
},{
    query: async function(db, queryParams){
        let cursor = await db.collection('upvotes').aggregate([
            {
                $group: {
                    "_id": {
                        "userId": "$userId",
                        "name": "$name"
                    },
                    "count": { $sum: 1}
                }
            },{
                $sort: {
                    "count": -1
                }
            },{
                $limit: 50
            }
        ], { 
            allowDiskUse: true
        })

        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }  
    }
},{
    query: async function(db, queryParams){
        let cursor = await db.collection('upvotes').aggregate([
            { 
                $group: {
                    "_id": {
                        "user": "$userId",
                        "ward": "$ward"
                    },
                    "count": { "$sum": 1 }
                }
            },{ 
                $group: {
                    "_id": {
                        "user": "$_id.user"
                    },
                    "uniqueCount": { "$sum": 1 }
                }
            },
            {
                $sort: {
                    "uniqueCount": -1
                }
            },
            {
                $limit: 50
            }
        ])
        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }       
    }
},{
    query: async function(db, queryParams){
        let cursor = await db.collection('upvotes').aggregate([
            { 
                $group: {
                    "_id": {
                        "tel": "$tel",
                        "service_request_number": "$service_request_number"
                    },
                    "number_of_names": { "$sum": 1 }
                }
            },{
                $match: {
                    "number_of_names": { $gte: 2 }
                }
            }
        ], { 
            allowDiskUse: true
        })
        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }       
    }
},{
    query: async function(db, queryParams){
        var name = queryParams.name;
        let cursor = await db.collection('upvotes').aggregate([
            {
                $match: {
                    "name": name
                }
            },{ 
                $group: {
                    "_id": "$name",
                    "wards": {
                        $addToSet: "$ward"
                    }
                }
            }
        ], { 
            allowDiskUse: true
        })
        const requestResults = await cursor.toArray();
        if (requestResults.length > 0) {
            return requestResults;
        }       
    }
}]


module.exports = {
    url: 'mongodb://localhost:27017/m149',
    db_queries
}