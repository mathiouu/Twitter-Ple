const hashtagsData = require('../hbase/hashtags')
exports.getTop10 = (req, res) => {
    const {day,start,end} =req.query;
  res.json(hashtagsData.getTopKHashtags(start,end));
};
