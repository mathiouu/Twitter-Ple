<template>
  <div class="home">
    <b-container style="margin: 0px; max-width: 100%">
      <b-row class="mt-2 mb-2">
        <b-col md="2">
          <h1>Categories</h1>

          <b-list-group>
            <!-- <router-link class="nav-link" v-bind:to="'/hashtags'"> -->
              <b-list-group-item v-for="table in data.ourTables" :key="table.name" >
                <!-- {{table.name}} -->

                <!-- Bad code but working -->
                <router-link class="nav-link" v-bind:to="'/hashtags'" v-if="table.name == 'seb-mat-hashtags'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/search-hashtags'" v-else-if="table.name == 'seb-mat-hashtagsByUsers'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/triplets'" v-else-if="table.name == 'seb-mat-hashtagsTriplets'">
                  {{table.name}}
                </router-link>

                <!-- UserQ2 -->

                <router-link class="nav-link" v-bind:to="'/tweetNbByCountry'" v-else-if="table.name == 'seb-mat-tweetByCountry'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/tweetNbByLang'" v-else-if="table.name == 'seb-mat-tweetByLang'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/userHashtags'" v-else-if="table.name == 'seb-mat-userHashtags'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/userNbTweet'" v-else-if="table.name == 'seb-mat-userNbTweet'">
                  {{table.name}}
                </router-link>

                <router-link class="nav-link" v-bind:to="'/search-hashtags'" v-else>
                  {{table.name}}
                </router-link>

              </b-list-group-item>
            <!-- </router-link> -->
          </b-list-group>
        </b-col>
        
        <b-col>
          <h4>Show all</h4>
          <b-list-group>
            <b-list-group-item v-for="table in data.allTables" :key="table.name" >
              {{table.name}}
            </b-list-group-item>
          </b-list-group>
        </b-col>
      </b-row>
      
    </b-container>
  </div>
</template>

<script>

import axios from 'axios';

export default {
  name: 'Home',
  data() {
    return {
      data : {
        ourTables : [],
        allTables : []
      },
      err : {
        boolValue : false,
        msg : ""
      }
    };
  },
  created() {
    this.initOurTables();
    this.initAllTables();
  },
  methods : {
    initOurTables(){
      const uri = `/api/hbase/ourTables`;

      axios.get(uri).then(response => {
        const dataRep = response.data;

        if(dataRep == 0){
          this.err.boolValue = true;
          this.err.msg = "Can't get our tables";
          return;
        }

        dataRep.forEach(elem => {
          this.data.ourTables.push(elem);
        });

      });

    },
    initAllTables(){
      const uri = `/api/hbase/allTables`;

      axios.get(uri).then(response => {
        const dataRep = response.data;

        if(dataRep == 0){
          this.err.boolValue = true;
          this.err.msg = "Can't get all tables";
          return;
        }

        dataRep.forEach(elem => {
          this.data.allTables.push(elem);
        });

      });
    }
  },
  computed: {
    
  },
};
</script>
