const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const history = require('connect-history-api-fallback');



const app = express();

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader(
    'Access-Control-Allow-Headers',
    'Origin, X-Requested-With, Content, Accept, Content-Type, Authorization'
  );
  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET, POST, PUT, DELETE, PATCH, OPTIONS'
  );
  next();
});
const port = 4000


app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})
// app.use(history({ index: 'index.html' }));

// app.use(express.static('./client/front/dist'));
// app.get('/', (req, res) => {
//   res.sendFile('index.html', {
//     root: `${__dirname}/client/front/dist/`,
//   });
// });


const hashtagsRoute = require('./server/routes/hashtags');
app.use('/api/hashtags/', hashtagsRoute);

const userQ2Route = require('./server/routes/userQ2Routes');
app.use('/api/users/', userQ2Route);

const influencersRoute = require('./server/routes/influencers');
app.use('/api/influencers/', influencersRoute);

const infoRoute = require('./server/routes/infoRoutes');
app.use('/api/hbase/', infoRoute);
