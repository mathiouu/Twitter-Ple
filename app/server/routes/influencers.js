const express = require('express');
const influencerCtrl = require('../controllers/influencers');

const router = express.Router();

router.get('/influencers', influencerCtrl.getInfluencers);
router.get('/triplets', influencerCtrl.getTriplets);
module.exports = router;
