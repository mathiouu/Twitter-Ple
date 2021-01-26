<template>
    <div>
        <b-row>
            <b-col md="2"></b-col>
            <b-col> 
                <b-list-group>
                    <b-list-group-item v-for="influencer in influencers" :key="influencer.user" >
                        {{influencer.user}}
                        <b-badge pill variant="primary">
                            {{influencer.times}}
                        </b-badge>
                        {{influencer.hashtags}}
                    </b-list-group-item>
                </b-list-group>

            </b-col>
            <b-col md="2"></b-col>


        </b-row>


    </div>
</template>

<script>
import axios from 'axios';
export default {
    data(){
        return{
            influencers:[]
        }
    },
    created(){
        this.initData();
    },
    methods:{
        initData(){
            axios
                .get(`/api/influencers/influencers`)
                .then((response) => {
                    this.feedData(response.data);
                    this.loaded = true;
                });
        },
        feedData(list){
            this.influencers= list.map(object =>{
                const {user, infos} = object;
                const {hashtags, times} = JSON.parse(infos.infos);
                
                return {user, times, hashtags };
            });
            this.influencers.sort((a,b)=> b.times - a.times);
        }

    }
}
</script>