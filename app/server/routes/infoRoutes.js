const express = require('express');
const router = express.Router();

const infoCtrl = require('../controllers/infoCtrl');    

router.get('/ourTables', infoCtrl.getOurTable);
router.get('/allTables', infoCtrl.getAllTables);

module.exports = router;
