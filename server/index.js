const hbase = require('hbase');
const client = hbase({
    host: '127.0.0.1',
    port: 8080
  });

client
  .table('testSmelezan' )
  .row('row1')
  .get('hashtags', function(err, cell){
    // Validate the result
    console.log(cell);
    console.log();
   });
