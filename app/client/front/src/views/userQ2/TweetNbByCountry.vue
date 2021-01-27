<template>
  <div>
    <b-row>
      <b-col md="2"> </b-col>
      <b-col md="2"> </b-col>
    </b-row>
    <b-row>
      <b-col md="10" v-if="!err.boolValue">
        <h2> Diagram about number tweets by country </h2>
        <TweetNbBar v-if="display.loaded" :labels="labels" :data="data" />
      </b-col>

      <b-col md="10" v-if="err.boolValue">
        <h2> {{this.err.msg}} </h2>
      </b-col>

      <b-col md="2">
        <b-form>
          <b-form-group id="input-group-1" label="topK (int): " label-for="input-1">
            <b-form-input id="input-1" v-model="form.topK" type="text"></b-form-input>
          </b-form-group>

          <b-button @click="submitTopK" variant="primary">Submit</b-button>
        </b-form>

        <b-form>
          <b-form-group id="input-group-1" label="Percentage (%): " label-for="input-1">
            <b-form-input id="input-1" v-model="form.percentage" type="text"></b-form-input>
          </b-form-group>

          <b-button @click="submitTopKPercentage" variant="primary">Submit</b-button>
        </b-form>
      </b-col>

    </b-row>

    <b-row>

      <b-col md="10" v-if="!err.boolValue">
        <h2> Pie chart about number tweets by country </h2>
        <TweetNbPie v-if="display.loaded" :labels="labels" :data="data" :colors="colors" />
      </b-col>

      <b-col md="2" v-if="display.stats">
        <h2> Stats </h2>
        <b-list-group>
          <b-list-group-item v-for="country in countryStats" :key="country.country" >
            {{country.country}} : {{country.average}} %
          </b-list-group-item>
        </b-list-group>
      </b-col>

    </b-row>
  </div>
</template>


<script>
import TweetNbBar from '../../components/charts/tweetNbBar'
import TweetNbPie from '../../components/charts/tweetNbPie'

import axios from "axios";
export default {
  components: {
    TweetNbBar,
    TweetNbPie
  },
  data() {
    return {
      form: {
        topK : "1",
        percentage : "1"
      },
      err : {
        boolValue : false,
        msg : ""  
      },
      display : {
        loaded: false,
        stats : false,  
      },
      labels: [],
      data: [],
      countryStats : [],
    };
  },
  created() {
    this.initDatas();
  },
  methods: {
    initDatas() {
      const stats = 'true';
      const uri = `/api/users/tweetNbByCountry?stats=${stats}`;

      axios.get(uri).then(response => {
        const dataRep = response.data;

        if(dataRep.length == 0){
          this.err.boolValue = true;
          this.err.msg = "Can't get datas";
          return;
        }

        dataRep.forEach(elem => {
          if(elem.average){
            this.labels.push(elem.country);
            this.data.push(parseInt(elem.times));
            const newElem = {
              ...elem,
              average : elem.average.toFixed(5)
            }
            this.countryStats.push(newElem);
          }
        });

        let colorList = [];
        for(var i = 0; i < this.data.length; i++){
          let color = this.getRandomColor();
          while(colorList.includes(color)){
            color = this.getRandomColor();
          }
          colorList.push(color);
        }
        
        this.colors = colorList;
        this.form.topK = dataRep.length;

        this.display.stats = true;
        this.display.loaded = true;
      });
    },
    submitTopK(){
      this.resetBooleanVal();
      this.resetChartData();

      const stats = 'true';
      const uri = `/api/users/getCountryTopKTweet?topk=${this.form.topK}&stats=${stats}`;

      this.resetChartData();

      axios.get(uri).then(response => {
        const dataRep = response.data;

        if(dataRep.length < this.form.topK){
          this.err.boolValue = true;
          this.err.msg = "Unvalid topk (too big)";
          return;
        }

        dataRep.forEach(elem => {
          if(elem.average){
            this.labels.push(elem.country);
            this.data.push(parseInt(elem.times));
            const newElem = {
              ...elem,
              average : elem.average.toFixed(5)
            }
            this.countryStats.push(newElem);
          }
        });

        this.display.stats = true;
        this.display.loaded = true;
      });
    },
    submitTopKPercentage(){
      this.resetBooleanVal();
      this.resetChartData();

      const stats = 'true';
      const uri = `/api/users/tweetNbByCountry?stats=${stats}`;

      if(this.form.percentage > 100 || this.form.percentage < 0){
        this.err.boolValue = true;
        this.err.msg = "Unvalid percentage (0 <= x% <= 100)";
        return;
      }

      axios.get(uri).then(response => {
        const dataRep = response.data;

        dataRep.forEach(elem => {
          if(elem.average){
            if(parseInt(elem.average) >= parseInt(this.form.percentage)){
              this.labels.push(elem.country);
              this.data.push(parseInt(elem.times));
              const newElem = {
                ...elem,
                average : elem.average.toFixed(5)
              }
              this.countryStats.push(newElem);
            }
          }
        });

        this.display.stats = true;
        this.display.loaded = true;
      });
    },
    resetChartData(){
      this.labels = [];
      this.data = [];
      this.countryStats = [];
    },
    resetBooleanVal(){
      this.display.loaded = false;
      this.err.boolValue = false;
      this.display.stats = false;
    },
    getRandomColor() {
      var letters = '0123456789ABCDEF';
      var color = '#';
      for (var i = 0; i < 6; i++) {
          color += letters[Math.floor(Math.random() * 16)];
      }
      return color;
    },
  },
};
</script>