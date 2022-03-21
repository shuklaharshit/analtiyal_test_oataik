const fs = require('mz/fs');
const csv = require('fast-csv');
const streamToIterator = require('stream-to-iterator');

const { Schema } = mongoose = require('mongoose');

const uri =
  "mongodb://test:test@cluster0-shard-00-00.wstp5.mongodb.net:27017,cluster0-shard-00-01.wstp5.mongodb.net:27017,cluster0-shard-00-02.wstp5.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-dy9xca-shard-0&authSource=admin&retryWrites=true&w=majority";


mongoose.Promise = global.Promise;
mongoose.set('debug', true);

const SampleSchema = new Schema({
  name: String,
  age: String,
  height: String,
  weight: String,
});

const SampleData = mongoose.model('SampleData', SampleSchema);

const log = data => console.log(JSON.stringify(data, undefined, 2));

(async function() {

  try {
    const conn = await mongoose.connect(uri);

    await Promise.all(Object.entries(conn.models).map(([k,m]) => m.remove()));

    let headers = Object.keys(SampleData.schema.paths)
      .filter(k => ['_id','__v'].indexOf(k) === -1);

    //console.log(headers);

    let stream = fs.createReadStream('./sample.csv')
      .pipe(csv({ headers }));

    const iterator = await streamToIterator(stream).init();

    let buffer = [],
        counter = 0;

    for ( let docPromise of iterator ) {
      let doc = await docPromise;
      buffer.push(doc);
      counter++;

      if ( counter > 10000 ) {
        await SampleData.insertMany(buffer);
        buffer = [];
        counter = 0;
      }
    }

    if ( counter > 0 ) {
      await SampleData.insertMany(buffer);
      buffer = [];
      counter = 0;
    }

  } catch(e) {
    console.error(e)
  } finally {
    process.exit()
  }

})()