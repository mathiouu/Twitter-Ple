<template>
    <div>
    <b-row>

        <b-col md="2"></b-col>
        <b-col>

          <b-form>
            <b-form-group
              id="input-group-1"
              label-for="input-1"
            >
              <b-form-input
                id="input-1"
                v-model="hashtagsToSearch"
                placeholder="Enter hashtags"
                required
              ></b-form-input>
              </b-form-group>
          <b-button @click="onSubmit" variant="primary">Submit</b-button>
          </b-form>
        </b-col>
        
        <b-col md="2"></b-col>
    </b-row>
    <b-row v-if="loaded">
        <b-col md="2"></b-col>
        <b-col md="8">
        <div class="accordion" role="tablist">
            <div v-for="(hashtag,counter) in hashtagsList" :key="hashtag.key">
                <hashtags-triplet-infos  v-bind="hashtag" :id="counter"/>
            </div>
        </div>

        </b-col>
        <b-col md="2"></b-col>
    </b-row>
  </div>
</template>

<script>

import axios from "axios";
import HashtagsTripletInfos from '../../components/HashtagsTripletInfos.vue';
export default {
  components: {
    HashtagsTripletInfos,
  },
  data() {
    return {
        hashtagsToSearch:'',
        loaded:false,
        hashtagsList: [],
    };
  },
  created() {
  },
  methods: {
    
    onSubmit() {
      console.log("submit");
      console.log(this.hashtagToSearch);
      axios
        .get(`/api/influencers/triplets?hashtags=${this.hashtagsToSearch}`)
        .then((response) => {
            this.hashtagsList = response.data;
            console.log(response.data);
            this.loaded = true;
        });
    },
  },
};
</script>