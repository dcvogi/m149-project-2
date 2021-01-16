const dbConfig = require('./api/config/development.config.js');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;


async function main(){
   const client = new MongoClient(dbConfig.url, {
      // retry to connect for 60 times
      reconnectTries: 60,
      // wait 1 second before retrying
      reconnectInterval: 1000
  });
   await client.connect();
   try {
      client.on('close', function(){
         console.log('Error closed')
      })
      const cursor = client.db("m149").collection("citizens").find();
      const results = await cursor.toArray();
      var totalUpvotes = [];
      if (results.length > 0) {
         console.log(`Found ${results.length} citizens`)
         for (var index=0; index<results.length; index++){
            if(!client.isConnected()){
               await client.connect();
               console.log('Reconnected...')
            }
            console.log(`User number: ${index}`)
            numberOfUpvotes = getRandomInt(600)
            if(!numberOfUpvotes || !results[index]){
               continue;
            }
            user = results[index]
            const requestCursor = await client.db("m149").collection("requests").aggregate([ { $sample: {size: numberOfUpvotes}}]);
            totalUpvotes = totalUpvotes.concat(await generateRandomUpvotes(client, user, requestCursor));
            if(totalUpvotes.length > 1000){
               await client.db("m149").collection("upvotes").insertMany(totalUpvotes, function(err, res){
                  if(err){
                     console.log(err)
                  }
                  else{
                     console.log(`Results uploaded: ${res.insertedCount}`)
                  }
               });
               totalUpvotes = []
            }
         };
      } else {
         console.log(`No citizens found`);
      }
      console.log(totalUpvotes.length)
   } catch (e) {
      console.error(e);
      client.close();
   }
   finally{
      client.close();
   }
}

main().catch(console.error);


async function generateRandomUpvotes(client, user, requestCursor){
   user['userId'] = user['_id']
   delete user['_id']
   
   const requestResults = await requestCursor.toArray();
   if (requestResults.length > 0) {
      let userUpvotes = []
      requestResults.forEach((result, i) => {
         result['requestId'] = result['_id']
         delete result['_id']

         userUpvotes.push({
            ...user,
            ...result
         });
      });
      
      return userUpvotes;
   } else {
       console.log(`No sample request results`);
   }
}

function getRandomInt(max) {
   return Math.floor(Math.random() * Math.floor(max));
 }

