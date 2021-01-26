<template>
  <div>
    <b-row>
      <b-col md="2"> </b-col>
      <b-col md="2"> </b-col>
    </b-row>
    <b-row>
        <b-col md="10">
            <h2> LE GRAPH </h2>
            <TopKHashtags v-if="loaded" :labels="labels" :data="data" />
        </b-col>
        <b-col md="10">
            <h2> LE GRAPH2 </h2>
            <TweetNb v-if="loaded" :labels="labels" :data="data" :colors="colors" />
            <!-- <TweetNb v-if="loaded" :labels="labels" :data="data" /> -->
        </b-col>
      <!-- <b-col md="2">
        <b-form>
          <b-form-group id="input-group-1" label="de: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.start"
              type="text"
            ></b-form-input>
          </b-form-group>e
          <b-form-group id="input-group-1" label="vers: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.end"
              type="text"
            ></b-form-input>
          </b-form-group>
          <b-button @click="onSubmit" variant="primary">Submit</b-button>
        </b-form>
      </b-col> -->
    </b-row>
  </div>
</template>


<script>
import TopKHashtags from '../../components/charts/topKHashtags'
import TweetNb from '../../components/charts/tweetNb'

import axios from "axios";
export default {
  components: {
    TopKHashtags,
    TweetNb
  },
  data() {
    return {
      labels: [],
      data: [],
      colors: [],
      loaded: false,
    };
  },
  created() {
    this.initDatas();
  },
  methods: {
    initDatas() {
        const uri = 'api/users/tweetNbByCountry';
        axios.get(uri).then(response => {
            const dataRep = response.data;

            this.labels = dataRep.map((data) => data.country);
            this.data = dataRep.map((data) => parseInt(data.times));
            this.loaded = true;

            let colorList = [];
            for(var i = 0; i < this.data.length; i++){
                colorList.push(this.getRandomColor());
            }
            this.colors = colorList;
        });
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