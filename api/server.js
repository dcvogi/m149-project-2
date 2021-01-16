const express = require('express');
const bodyParser = require('body-parser');

const dbConfig = require('./config/development.config.js');
const mongodb = require('mongodb');
const querystring = require('querystring');



// create express app
const app = express();
const PORT = 5000;

// parse requests of content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }))

// parse requests of content-type - application/json
app.use(bodyParser.json())

mongodb.connect(
   dbConfig.url,
   {  useNewUrlParser: true, 
      useUnifiedTopology: true },
   function (err, client) {
      db = client.db()
      // listen for requests
      app.listen(PORT, () => {
         console.log(`Server is listening on port ${PORT}`);
      });
   }
)

// define a simple route
app.get('/', (req, res) => {
   res.json({"message": "Welcome to M149 Project 2 application."});
});

app.post('/create-request', async function (req, res) {
   // Sending request to create a data
   let dataToUpload = req.body;
   dataToUpload['status'] = dataToUpload['status'] || 'Open';
   dataToUpload['creation_date'] = dataToUpload['creation_date'] || new Date().toISOString().replace('Z', '');
   dataToUpload['service_request_number'] = dataToUpload['service_request_number'] || "1"+ Math.floor(Math.random() * 9) + "-" + Date.now().toString().slice(0, -5);

   if(!dataToUpload['location'] && dataToUpload['longitude'] && dataToUpload['latitude']){
      dataToUpload['location'] = {
         "type": "Point",
         "coordinates": [dataToUpload['longitude'], dataToUpload['latitude']]
      }
   }

   await db.collection('requests').insertOne(dataToUpload, function (err, info) {
      res.json(info.ops[0])
   });
})

app.post('/upvote', async function (req, res) {
   let userDocumentId;
   let requestDocumentId;

   //Search for user
   const citizen = await db.collection("citizens").findOne({ name: req.body.name});
   if (citizen) {
      user = citizen
   } else {
      res.json({
         "error": "No user found"
      })
   }

   //Search for incident
   const request = await db.collection("requests").findOne({ service_request_number: req.body.request_number});
   if (request) {
      requestDocument = request
   } else {
      res.json({
         "error": "No incident found"
      })
   }

   if(user && requestDocument){
      // Sending request to create a upvote
      user['userId'] = user['_id']
      delete user['_id']

      requestDocument['requestId'] = requestDocument['_id']
      delete requestDocument['_id']
      
      await db.collection('upvotes').insertOne({ 
         ...user,
         ...requestDocument
      }, function (err, info) {
         res.json(info.ops[0])
      });
   }
})

//SHOW QUERY PAGE
app.get('/query/:qid', async (req, res) => {
   const { qid, pid } = req.params;
   
   try {
     query = dbConfig.db_queries[qid].query;
     res.json(await query(db, req.query))
   }
   catch (e) {
     console.log("Error: ", e);
     res.json("Error: ", e);
   }
 });