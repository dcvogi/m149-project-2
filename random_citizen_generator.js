var faker = require('faker');
var fs = require('fs');

const numberOfCitizens = 6000;

var citizens = [];

for(var index=0; index<numberOfCitizens; index++){
    citizens.push({
        "name": faker.name.findName(),
        "tel": faker.phone.phoneNumberFormat(),
        "address": faker.address.streetAddress()
    })
}

fs.writeFile("citizens.json", JSON.stringify(citizens), function(err) {
        if (err) throw err;
        console.log('complete');
    }
);
