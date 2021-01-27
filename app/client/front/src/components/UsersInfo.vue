<template>
    <b-card no-body class="mb-1">
      <b-card-header header-tag="header" class="p-1" role="tab">
        <b-button block v-b-toggle="`accordion-${id}`" variant="Light">
            <!-- {{usersObj.user}} -->
            <a :href="'https://twitter.com/' + usersObj.user"> {{usersObj.user}} </a>
        </b-button>
      </b-card-header>

      <b-collapse :id="`accordion-${id}`" accordion="my-accordion" role="tabpanel">
        <b-card-body>
          <b-card-text>
            {{usersObj.hashTags}}
          </b-card-text>
        </b-card-body>
      </b-collapse>

    </b-card>
</template>

<script>
  export default {
    props:{
        users : Object,
        id : Number
    },
    created(){
    },
    data() {
      return {
        usersObj : this.users,

      }
    }
  }
</script>